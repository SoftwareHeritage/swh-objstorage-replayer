# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools

from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists, sets, binary

from swh.model.hypothesis_strategies import present_contents
from swh.objstorage import get_objstorage
from swh.objstorage.replayer.replay import (
    is_hash_in_bytearray,
    process_replay_objects_content,
)
from swh.journal.tests.utils import MockedJournalClient, MockedKafkaWriter


hash_strategy = binary(min_size=20, max_size=20)


@settings(max_examples=500)
@given(
    sets(hash_strategy, min_size=0, max_size=500), sets(hash_strategy, min_size=10),
)
def test_is_hash_in_bytearray(haystack, needles):
    array = b"".join(sorted(haystack))
    needles |= haystack  # Exhaustively test for all objects in the array
    for needle in needles:
        assert is_hash_in_bytearray(needle, array, len(haystack)) == (
            needle in haystack
        )


@given(lists(present_contents(), min_size=1))
@settings(suppress_health_check=[HealthCheck.too_slow])
def test_replay_content(objects):

    queue = []
    replayer = MockedJournalClient(queue)
    writer = MockedKafkaWriter(queue)

    objstorage1 = get_objstorage(cls="memory", args={})
    objstorage2 = get_objstorage(cls="memory", args={})

    contents = []
    for obj in objects:
        objstorage1.add(obj.data)
        contents.append(obj)
        writer.write_addition("content", obj)

    # Bail out early if we didn't insert any relevant objects...
    queue_size = len(queue)
    assert queue_size != 0, "No test objects found; hypothesis strategy bug?"

    assert replayer.stop_after_objects is None
    replayer.stop_after_objects = queue_size

    worker_fn = functools.partial(
        process_replay_objects_content, src=objstorage1, dst=objstorage2
    )

    replayer.process(worker_fn)

    # only content with status visible will be copied in storage2
    expected_objstorage_state = {
        c.sha1: c.data for c in contents if c.status == "visible"
    }

    assert expected_objstorage_state == objstorage2.state
