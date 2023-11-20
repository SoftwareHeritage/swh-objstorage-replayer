# Copyright (C) 2022-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from queue import Queue

from swh.journal.client import JournalClient
from swh.journal.writer import get_journal_writer
from swh.model.model import Content
from swh.objstorage import factory
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.multiplexer.filter.filter import ObjStorageFilter
from swh.objstorage.replayer import replay
from swh.objstorage.replayer.replay import copy_object  # needed for MonkeyPatch
from swh.objstorage.replayer.tests.test_cli import (
    _patch_objstorages as patch_objstorages,
)

CONTENTS = [Content.from_data(f"foo{i}".encode()) for i in range(10)] + [
    Content.from_data(f"forbidden foo{i}".encode(), status="hidden") for i in range(10)
]


class FailingObjstorage(ObjStorageFilter):
    def __init__(self, storage):
        super().__init__(storage)
        self.calls = defaultdict(lambda: 0)
        self.rate = 3

    def get(self, obj_id, *args, **kwargs):
        self.calls[obj_id] += 1
        if (self.calls[obj_id] % self.rate) == 0:
            return self.storage.get(obj_id, *args, **kwargs)
        raise Exception("Nope")

    def add(self, content, obj_id, check_presence=True, *args, **kwargs):
        self.calls[obj_id] += 1
        if (self.calls[obj_id] % self.rate) == 0:
            return self.storage.add(content, obj_id, check_presence, *args, **kwargs)
        raise Exception("Nope")


class NotFoundObjstorage(ObjStorageFilter):
    def get(self, obj_id, *args, **kwargs):
        raise ObjNotFoundError(obj_id)


def prepare_test(kafka_server, kafka_prefix, kafka_consumer_group):
    src_objstorage = factory.get_objstorage(cls="mocked", name="src")

    writer = get_journal_writer(
        cls="kafka",
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        anonymize=False,
    )

    for content in CONTENTS:
        src_objstorage.add(content.data, obj_id=content.sha1)
        writer.write_addition("content", content)

    replayer = JournalClient(
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        stop_on_eof=True,
    )

    return replayer, src_objstorage


def copy_object_q(q):
    """Wrap the original copy_object function to capture (thread-local) tenacity
    stats and puch them in a queue suitable for checking in a test session"""

    def wrap(obj_id, src, dst):
        try:
            ret = copy_object(obj_id, src, dst)
            return ret
        finally:
            q.put(("get", obj_id, replay.get_object.retry.statistics.copy()))
            q.put(("put", obj_id, replay.put_object.retry.statistics.copy()))

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
    monkeypatch.setattr(replay, "copy_object", copy_object_q(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
    ) as replayer:
        client.process(replayer.replay)

    # only content with status visible will be copied in storage2
    expected_objstorage_state = {
        c.sha1: c.data for c in CONTENTS if c.status == "visible"
    }
    assert expected_objstorage_state == dst_objstorage.state

    stats = [q.get_nowait() for i in range(q.qsize())]
    for obj_id in expected_objstorage_state:
        put = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "put"
        )
        assert put.get("attempt_number") == 1
        assert put.get("start_time") > 0
        assert put.get("idle_for") == 0

        get = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "get"
        )
        assert get.get("attempt_number") == 3
        assert get.get("start_time") > 0
        assert get.get("idle_for") > 0
        assert get.get("delay_since_first_attempt") > 0


@patch_objstorages(["src", "dst"])
def test_replay_content_with_errors(
    objstorages, kafka_server, kafka_prefix, kafka_consumer_group, monkeypatch
):
    client, src_objstorage = prepare_test(
        kafka_server, kafka_prefix, kafka_consumer_group
    )
    dst_objstorage = objstorages["dst"]
    objstorages["src"] = FailingObjstorage(src_objstorage)

    q = Queue()
    monkeypatch.setattr(replay, "copy_object", copy_object_q(q))
    monkeypatch.setattr(replay.get_object.retry.stop, "max_attempt_number", 2)

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
    ) as replayer:
        client.process(replayer.replay)

    # no object could be replicated
    assert dst_objstorage.state == {}

    stats = [q.get_nowait() for i in range(q.qsize())]
    for obj in CONTENTS:
        if obj.status != "visible":
            continue

        obj_id = obj.sha1
        put = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "put"
        )
        assert put == {}

        get = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "get"
        )
        assert get.get("attempt_number") == 2
        assert get.get("start_time") > 0
        assert get.get("idle_for") > 0
        assert get.get("delay_since_first_attempt") > 0


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
    monkeypatch.setattr(replay, "copy_object", copy_object_q(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
    ) as replayer:
        client.process(replayer.replay)

    # no object could be replicated
    assert dst_objstorage.state == {}

    stats = [q.get_nowait() for i in range(q.qsize())]
    for obj in CONTENTS:
        if obj.status != "visible":
            continue

        obj_id = obj.sha1
        put = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "put"
        )
        assert put == {}

        get = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "get"
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
    monkeypatch.setattr(replay, "copy_object", copy_object_q(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
    ) as replayer:
        client.process(replayer.replay)

    # only content with status visible will be copied in storage2
    expected_objstorage_state = {
        c.sha1: c.data for c in CONTENTS if c.status == "visible"
    }
    assert expected_objstorage_state == dst_objstorage.storage.state

    stats = [q.get_nowait() for i in range(q.qsize())]
    for obj_id in expected_objstorage_state:
        put = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "put"
        )
        assert put.get("attempt_number") == 3
        assert put.get("start_time") > 0
        assert put.get("idle_for") > 0
        assert put.get("delay_since_first_attempt") > 0

        get = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "get"
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
    monkeypatch.setattr(replay, "copy_object", copy_object_q(q))
    monkeypatch.setattr(replay.get_object.retry.stop, "max_attempt_number", 2)

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
    ) as replayer:
        client.process(replayer.replay)

    # no object could be replicated
    assert dst_objstorage.storage.state == {}

    stats = [q.get_nowait() for i in range(q.qsize())]
    for obj in CONTENTS:
        if obj.status != "visible":
            continue

        obj_id = obj.sha1
        put = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "put"
        )
        assert put.get("attempt_number") == 2
        assert put.get("start_time") > 0
        assert put.get("idle_for") > 0
        assert put.get("delay_since_first_attempt") > 0

        get = next(
            stat for (meth, oid, stat) in stats if oid == obj_id and meth == "get"
        )
        assert get.get("attempt_number") == 1
        assert get.get("start_time") > 0
        assert get.get("idle_for") == 0
