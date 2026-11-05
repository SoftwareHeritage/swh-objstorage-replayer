# Copyright (C) 2022-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from queue import Queue
from typing import Tuple, cast

from swh.journal.client import EofBehavior, JournalClient
from swh.journal.writer import get_journal_writer
from swh.model.hashutil import HashDict
from swh.model.model import Content
from swh.objstorage import factory
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import ObjStorage
from swh.objstorage.replayer import replay
from swh.objstorage.replayer.replay import (  # needed for MonkeyPatch
    format_obj_id,
    get_objects,
    put_object,
)
from swh.objstorage.replayer.tests.test_cli import (
    _patch_objstorages as patch_objstorages,
)

CONTENTS = [Content.from_data(f"foo{i}".encode()) for i in range(10)] + [
    Content.from_data(f"forbidden foo{i}".encode(), status="hidden") for i in range(10)
]


class ObjStorageProxyMixin(ObjStorage):
    def __init__(self, storage):
        super().__init__()
        self.storage = storage

    def delete(self, obj_id):
        pass

    def _state_key(self, obj_id):
        return self.storage._state_key(obj_id)

    def __contains__(self, *args, **kwargs):
        return self.storage.__contains__(*args, **kwargs)

    def __iter__(self):
        return self.storage.__iter__()


class FailingObjstorage(ObjStorageProxyMixin):
    def __init__(self, storage):
        super().__init__(storage)
        self.calls = defaultdict(lambda: 0)
        self.rate = 3

    def get(self, obj_id, *args, **kwargs):
        raise Exception(
            "Called FailingObjstorage.get instead of FailingObjstorage.get_batch"
        )

    def get_batch(self, obj_ids, *args, **kwargs):
        key = ", ".join(map(format_obj_id, obj_ids))
        self.calls[key] += 1
        if (self.calls[key] % self.rate) == 0:
            return self.storage.get_batch(obj_ids, *args, **kwargs)
        raise Exception("Nope")

    def add(self, content, obj_id, check_presence=True, *args, **kwargs):
        self.calls[format_obj_id(obj_id)] += 1
        if (self.calls[format_obj_id(obj_id)] % self.rate) == 0:
            return self.storage.add(content, obj_id, check_presence, *args, **kwargs)
        raise Exception("Nope")


class NotFoundObjstorage(ObjStorageProxyMixin):
    def get(self, obj_id, *args, **kwargs):
        raise ObjNotFoundError(obj_id)


def prepare_test(
    kafka_server, kafka_prefix, kafka_consumer_group
) -> Tuple[JournalClient, ObjStorageInterface]:
    src_objstorage = factory.get_objstorage(cls="mocked", name="src")

    writer = get_journal_writer(
        cls="kafka",
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        anonymize=False,
    )

    for content in CONTENTS:
        assert content.data is not None
        src_objstorage.add(content.data, obj_id=cast(HashDict, content.hashes()))
        writer.write_addition("content", content)

    replayer = JournalClient(
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
    )

    return replayer, src_objstorage


def get_objects_q(q):
    """Wrap the original ``get_objects`` function to capture (thread-local) tenacity
    stats and push them in a queue suitable for checking in a test session"""

    def wrap(obj_ids, *args, **kwargs):
        replay.get_objects.statistics = {}
        try:
            ret = get_objects(obj_ids, *args, **kwargs)
            return ret
        finally:
            q.put(("get", obj_ids, get_objects.statistics))

    return wrap


def put_object_q(q):
    """Wrap the original ``put_object`` function to capture (thread-local) tenacity
    stats and push them in a queue suitable for checking in a test session"""

    def wrap(obj_id, *args, **kwargs):
        put_object.statistics = {}
        try:
            ret = put_object(obj_id, *args, **kwargs)
            return ret
        finally:
            q.put(("put", obj_id, put_object.statistics))

    return wrap


@patch_objstorages(["src", "dst"])
def test_replay_content_with_transient_errors(
    objstorages, kafka_server, kafka_prefix, kafka_consumer_group, monkeypatch
):
    client, src_objstorage = prepare_test(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    dst_objstorage = objstorages["dst"]
    objstorages["src"] = FailingObjstorage(src_objstorage)

    q = Queue()
    monkeypatch.setattr(replay, "get_objects", get_objects_q(q))
    monkeypatch.setattr(replay, "put_object", put_object_q(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
        min_batch_size=1,
        max_batch_size=1,
    ) as replayer:
        client.process(replayer.replay)

    # only content with status visible will be copied in storage2
    expected_objstorage_state = {
        dst_objstorage._state_key(c.hashes()): c.with_data().data
        for c in CONTENTS
        if c.status == "visible"
    }
    assert expected_objstorage_state == dst_objstorage.state

    stats = [q.get_nowait() for i in range(q.qsize())]
    for state_key in expected_objstorage_state:
        put = next(
            stat
            for (meth, oid, stat) in stats
            if meth == "put" and dst_objstorage._state_key(oid) == state_key
        )
        assert put.get("attempt_number") == 1
        assert put.get("start_time") > 0
        assert put.get("idle_for") == 0

        get = next(
            stat
            for (meth, oids, stat) in stats
            if meth == "get" and dst_objstorage._state_key(oids[0]) == state_key
        )
        assert get.get("attempt_number") == 3
        assert get.get("start_time") > 0
        assert get.get("idle_for") > 0
        assert get.get("delay_since_first_attempt") > 0


@patch_objstorages(["src", "dst"])
def test_replay_content_with_errors(
    objstorages,
    kafka_server,
    kafka_prefix,
    kafka_consumer_group,
    monkeypatch,
    mocker,
):
    import sentry_sdk

    sentry_sdk.init()
    capture_event = mocker.spy(sentry_sdk.Hub.current.client, "capture_event")

    client, src_objstorage = prepare_test(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    dst_objstorage = objstorages["dst"]
    objstorages["src"] = FailingObjstorage(src_objstorage)

    q = Queue()
    monkeypatch.setattr(replay, "get_objects", get_objects_q(q))
    monkeypatch.setattr(replay, "put_object", put_object_q(q))
    monkeypatch.setattr(get_objects.retry.stop, "max_attempt_number", 2)

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
        min_batch_size=1,
        max_batch_size=1,
    ) as replayer:
        client.process(replayer.replay)

    # no object could be replicated
    assert dst_objstorage.state == {}
    assert capture_event.mock_calls

    stats = [q.get_nowait() for i in range(q.qsize())]
    assert [
        stat for (meth, oid, stat) in stats if meth == "put"
    ] == [], "put_object was called"

    for obj in CONTENTS:
        if obj.status != "visible":
            continue

        obj_id = obj.hashes()

        get = next(
            stat for (meth, oids, stat) in stats if meth == "get" and oids == [obj_id]
        )
        assert get.get("attempt_number") == 2
        assert get.get("start_time") > 0
        assert get.get("idle_for") > 0
        assert get.get("delay_since_first_attempt") > 0

        # check hexadecimal hashes are available in sentry event extra data
        hex_objid = {algo: hash.hex() for algo, hash in obj_id.items()}
        assert any(
            mock_call.kwargs["event"]["extra"].get("obj_ids") == [hex_objid]
            for mock_call in capture_event.mock_calls
        ), [
            mock_call.kwargs["event"]["extra"] for mock_call in capture_event.mock_calls
        ]


@patch_objstorages(["src", "dst"])
def test_replay_content_not_found(
    objstorages, kafka_server, kafka_prefix, kafka_consumer_group, monkeypatch
):
    client, src_objstorage = prepare_test(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    dst_objstorage = objstorages["dst"]
    objstorages["src"] = NotFoundObjstorage(src_objstorage)

    q = Queue()
    monkeypatch.setattr(replay, "get_objects", get_objects_q(q))
    monkeypatch.setattr(replay, "put_object", put_object_q(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
        min_batch_size=1,
        max_batch_size=1,
    ) as replayer:
        client.process(replayer.replay)

    assert dst_objstorage.state == {}, "some objects were replicated"

    stats = [q.get_nowait() for i in range(q.qsize())]

    assert [
        stat for (meth, oid, stat) in stats if meth == "put"
    ] == [], "put_object was called"

    for obj in CONTENTS:
        if obj.status != "visible":
            continue

        obj_id = obj.hashes()

        get = next(
            stat for (meth, oids, stat) in stats if meth == "get" and oids == [obj_id]
        )
        # ObjectNotFound should not be retried several times...
        assert get.get("attempt_number") == 1
        assert get.get("start_time") > 0
        assert get.get("idle_for") == 0


@patch_objstorages(["src", "dst"])
def test_replay_content_with_transient_add_errors(
    objstorages, kafka_server, kafka_prefix, kafka_consumer_group, monkeypatch
):
    client, src_objstorage = prepare_test(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    objstorages["dst"] = FailingObjstorage(objstorages["dst"])
    dst_objstorage = objstorages["dst"]

    q = Queue()
    monkeypatch.setattr(replay, "get_objects", get_objects_q(q))
    monkeypatch.setattr(replay, "put_object", put_object_q(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
        min_batch_size=1,
        max_batch_size=1,
    ) as replayer:
        client.process(replayer.replay)

    # only content with status visible will be copied in storage2
    expected_objstorage_state = {
        dst_objstorage._state_key(c.hashes()): c.data
        for c in CONTENTS
        if c.status == "visible"
    }
    assert expected_objstorage_state == dst_objstorage.storage.state

    stats = [q.get_nowait() for i in range(q.qsize())]
    for state_key in expected_objstorage_state:
        put = next(
            stat
            for (meth, oid, stat) in stats
            if meth == "put" and dst_objstorage._state_key(oid) == state_key
        )
        assert put.get("attempt_number") == 3
        assert put.get("start_time") > 0
        assert put.get("idle_for") > 0
        assert put.get("delay_since_first_attempt") > 0

        get = next(
            stat
            for (meth, oids, stat) in stats
            if meth == "get" and dst_objstorage._state_key(oids[0]) == state_key
        )
        assert get.get("attempt_number") == 1
        assert get.get("start_time") > 0
        assert get.get("idle_for") == 0


@patch_objstorages(["src", "dst"])
def test_replay_content_with_add_errors(
    objstorages, kafka_server, kafka_prefix, kafka_consumer_group, monkeypatch
):
    client, src_objstorage = prepare_test(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    objstorages["dst"] = FailingObjstorage(objstorages["dst"])
    dst_objstorage = objstorages["dst"]

    q = Queue()
    monkeypatch.setattr(replay, "get_objects", get_objects_q(q))
    monkeypatch.setattr(replay, "put_object", put_object_q(q))
    monkeypatch.setattr(get_objects.retry.stop, "max_attempt_number", 2)

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
        min_batch_size=1,
        max_batch_size=1,
    ) as replayer:
        client.process(replayer.replay)

    # no object could be replicated
    assert dst_objstorage.storage.state == {}

    stats = [q.get_nowait() for i in range(q.qsize())]
    for obj in CONTENTS:
        if obj.status != "visible":
            continue

        obj_id = obj.hashes()
        put = next(
            stat for (meth, oid, stat) in stats if meth == "put" and oid == obj_id
        )
        assert put.get("attempt_number") == 2
        assert put.get("start_time") > 0
        assert put.get("idle_for") > 0
        assert put.get("delay_since_first_attempt") > 0

        get = next(
            stat for (meth, oids, stat) in stats if meth == "get" and oids == [obj_id]
        )
        assert get.get("attempt_number") == 1
        assert get.get("start_time") > 0
        assert get.get("idle_for") == 0
