# Copyright (C) 2022-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from collections import defaultdict
from queue import Queue
from typing import Tuple, cast

from tenacity import RetryCallState

from swh.journal.client import EofBehavior, JournalClient
from swh.journal.writer import get_journal_writer
from swh.model.hashutil import HashDict
from swh.model.model import Content
from swh.objstorage import factory
from swh.objstorage.exc import ObjNotFoundError
from swh.objstorage.interface import ObjStorageInterface
from swh.objstorage.objstorage import ObjStorage
from swh.objstorage.replayer import replay
from swh.objstorage.replayer.replay import format_obj_id
from swh.objstorage.replayer.tests.test_cli import (
    _patch_objstorages as patch_objstorages,
)

CONTENTS = [Content.from_data(f"foo{i}".encode()) for i in range(10)] + [
    Content.from_data(f"forbidden foo{i}".encode(), status="hidden") for i in range(10)
]
# CONTENTS = CONTENTS[:4]


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
        self.calls[format_obj_id(obj_id)] += 1
        if (self.calls[format_obj_id(obj_id)] % self.rate) == 0:
            return self.storage.get(obj_id, *args, **kwargs)
        raise Exception("GET: Nope")

    def add(self, content, obj_id, check_presence=True, *args, **kwargs):
        self.calls[format_obj_id(obj_id)] += 1
        if (self.calls[format_obj_id(obj_id)] % self.rate) == 0:
            return self.storage.add(content, obj_id, check_presence, *args, **kwargs)
        raise Exception("ADD: Nope")


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


def check_stats(stats, expected_attempts):
    """Utility checker for expected tenacity stats"""
    assert max(entry.get("attempt_number", 0) for entry in stats) == expected_attempts
    entry = next(
        entry for entry in stats if entry["attempt_number"] == expected_attempts
    )
    assert entry["start_time"] > 0
    if expected_attempts > 1:
        assert entry["idle_for"] > 0
    else:
        assert entry["idle_for"] == 0


def wrap_retry_callback(q):
    def retry_cb(retry_state: RetryCallState):
        assert retry_state.fn is not None
        fn = retry_state.fn.__name__.split(".")[-1]
        if fn in ("put_object", "get_object"):
            q.put(
                (
                    fn,
                    retry_state.args[1],
                    {
                        "attempt_number": retry_state.attempt_number,
                        "idle_for": retry_state.idle_for,
                        "start_time": retry_state.start_time,
                        "delay_since_first_attempt": retry_state.seconds_since_start,
                    },
                )
            )

    return retry_cb


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
    monkeypatch.setattr(replay, "retry_cb", wrap_retry_callback(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
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
        puts = [
            stat
            for (meth, oid, stat) in stats
            if dst_objstorage._state_key(oid) == state_key and meth == "put_object"
        ]
        check_stats(puts, 1)

        gets = [
            stat
            for (meth, oid, stat) in stats
            if dst_objstorage._state_key(oid) == state_key and meth == "get_object"
        ]
        check_stats(gets, 3)


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
    monkeypatch.setattr(replay, "retry_cb", wrap_retry_callback(q))
    monkeypatch.setattr(replay.get_object.retry.stop, "max_attempt_number", 2)

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
    ) as replayer:
        client.process(replayer.replay)

    stats = [q.get_nowait() for i in range(q.qsize())]

    # no object could be replicated
    assert dst_objstorage.state == {}
    assert capture_event.mock_calls

    for obj in CONTENTS:
        if obj.status != "visible":
            continue

        obj_id = obj.hashes()
        puts = [
            stat
            for (meth, oid, stat) in stats
            if oid == obj_id and meth == "put_object"
        ]
        assert puts == []

        gets = [
            stat
            for (meth, oid, stat) in stats
            if oid == obj_id and meth == "get_object"
        ]
        check_stats(gets, 2)

        # check hexadecimal hashes are available in sentry event extra data
        hex_objid = {algo: hash.hex() for algo, hash in obj_id.items()}
        assert any(
            mock_call.kwargs["event"]["extra"].get("obj_id") == hex_objid
            for mock_call in capture_event.mock_calls
        )


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
    monkeypatch.setattr(replay, "retry_cb", wrap_retry_callback(q))

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

        obj_id = obj.hashes()
        puts = [
            stat
            for (meth, oid, stat) in stats
            if oid == obj_id and meth == "put_object"
        ]
        assert puts == []

        gets = [
            stat
            for (meth, oid, stat) in stats
            if oid == obj_id and meth == "get_object"
        ]
        # ObjectNotFound should not be retried several times...
        check_stats(gets, 1)


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
    monkeypatch.setattr(replay, "retry_cb", wrap_retry_callback(q))

    with replay.ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
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
        puts = [
            stat
            for (meth, oid, stat) in stats
            if dst_objstorage._state_key(oid) == state_key and meth == "put_object"
        ]
        check_stats(puts, 3)

        gets = [
            stat
            for (meth, oid, stat) in stats
            if dst_objstorage._state_key(oid) == state_key and meth == "get_object"
        ]
        check_stats(gets, 1)


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
    monkeypatch.setattr(replay, "retry_cb", wrap_retry_callback(q))
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

        obj_id = obj.hashes()
        puts = [
            stat
            for (meth, oid, stat) in stats
            if oid == obj_id and meth == "put_object"
        ]
        check_stats(puts, 2)

        gets = [
            stat
            for (meth, oid, stat) in stats
            if oid == obj_id and meth == "get_object"
        ]
        check_stats(gets, 1)
