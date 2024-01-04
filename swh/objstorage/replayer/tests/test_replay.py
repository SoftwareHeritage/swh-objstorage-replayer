# Copyright (C) 2019-2023 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from hypothesis import given, settings
from hypothesis.strategies import sets

from swh.journal.client import EofBehavior, JournalClient
from swh.journal.writer import get_journal_writer
from swh.model.hypothesis_strategies import sha1
from swh.model.model import Content
from swh.objstorage.replayer.replay import ContentReplayer, is_hash_in_bytearray
from swh.objstorage.replayer.tests.test_cli import (
    _patch_objstorages as patch_objstorages,
)

CONTENTS = [Content.from_data(f"foo{i}".encode()) for i in range(10)] + [
    Content.from_data(f"forbidden foo{i}".encode(), status="hidden") for i in range(10)
]


@settings(max_examples=500)
@given(
    sets(sha1(), min_size=0, max_size=500),
    sets(sha1(), min_size=10),
)
def test_is_hash_in_bytearray(haystack, needles):
    array = b"".join(sorted(haystack))
    needles |= haystack  # Exhaustively test for all objects in the array
    for needle in needles:
        assert is_hash_in_bytearray(needle, array, len(haystack)) == (
            needle in haystack
        )


@patch_objstorages(["src", "dst"])
def test_replay_content(objstorages, kafka_server, kafka_prefix, kafka_consumer_group):
    objstorage1 = objstorages["src"]
    objstorage2 = objstorages["dst"]

    writer = get_journal_writer(
        cls="kafka",
        brokers=[kafka_server],
        client_id="kafka_writer",
        prefix=kafka_prefix,
        anonymize=False,
    )

    for content in CONTENTS:
        objstorage1.add(content.data, obj_id=content.hashes())
        writer.write_addition("content", content)

    client = JournalClient(
        brokers=kafka_server,
        group_id=kafka_consumer_group,
        prefix=kafka_prefix,
        on_eof=EofBehavior.STOP,
    )

    with ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
    ) as replayer:
        client.process(replayer.replay)
    # only content with status visible will be copied in storage2
    expected_objstorage_state = {
        objstorage2._state_key(c.hashes()): c.data
        for c in CONTENTS
        if c.status == "visible"
    }

    assert expected_objstorage_state == objstorage2.state


@patch_objstorages(["src", "dst"])
def test_replay_exclude(objstorages):
    src = objstorages["src"]
    dst = objstorages["dst"]
    cnt1 = b"foo bar"
    cnt2 = b"baz qux"
    id1 = Content.from_data(cnt1).hashes()
    id2 = Content.from_data(cnt2).hashes()
    src.add(b"foo bar", obj_id=id1)
    src.add(b"baz qux", obj_id=id2)
    kafka_partitions = {
        "content": [
            {
                **id1,
                "status": "visible",
            },
            {
                **id2,
                "status": "visible",
            },
        ]
    }
    with ContentReplayer(
        src={"cls": "mocked", "name": "src"},
        dst={"cls": "mocked", "name": "dst"},
        exclude_fn=lambda obj: obj["sha1"] == id1["sha1"],
        concurrency=1,
    ) as replayer:
        replayer.replay(kafka_partitions)

    assert id1 not in dst
    assert id2 in dst
