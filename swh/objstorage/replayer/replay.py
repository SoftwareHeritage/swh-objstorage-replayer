# Copyright (C) 2019-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from time import time
from typing import Callable, Dict, List, Optional

from sentry_sdk import capture_exception, push_scope

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)

from swh.core.statsd import statsd
from swh.model.hashutil import hash_to_hex
from swh.model.model import SHA1_SIZE
from swh.objstorage.objstorage import ID_HASH_ALGO, ObjNotFoundError, ObjStorage

logger = logging.getLogger(__name__)

CONTENT_OPERATIONS_METRIC = "swh_content_replayer_operations_total"
CONTENT_RETRY_METRIC = "swh_content_replayer_retries_total"
CONTENT_BYTES_METRIC = "swh_content_replayer_bytes"
CONTENT_DURATION_METRIC = "swh_content_replayer_duration_seconds"


def is_hash_in_bytearray(hash_, array, nb_hashes, hash_size=SHA1_SIZE):
    """
    Checks if the given hash is in the provided `array`. The array must be
    a *sorted* list of sha1 hashes, and contain `nb_hashes` hashes
    (so its size must by `nb_hashes*hash_size` bytes).

    Args:
        hash_ (bytes): the hash to look for
        array (bytes): a sorted concatenated array of hashes (may be of
            any type supporting slice indexing, eg. :class:`mmap.mmap`)
        nb_hashes (int): number of hashes in the array
        hash_size (int): size of a hash (defaults to 20, for SHA1)

    Example:

    >>> import os
    >>> hash1 = os.urandom(20)
    >>> hash2 = os.urandom(20)
    >>> hash3 = os.urandom(20)
    >>> array = b''.join(sorted([hash1, hash2]))
    >>> is_hash_in_bytearray(hash1, array, 2)
    True
    >>> is_hash_in_bytearray(hash2, array, 2)
    True
    >>> is_hash_in_bytearray(hash3, array, 2)
    False
    """
    if len(hash_) != hash_size:
        raise ValueError("hash_ does not match the provided hash_size.")

    def get_hash(position):
        return array[position * hash_size : (position + 1) * hash_size]

    # Regular dichotomy:
    left = 0
    right = nb_hashes
    while left < right - 1:
        middle = int((right + left) / 2)
        pivot = get_hash(middle)
        if pivot == hash_:
            return True
        elif pivot < hash_:
            left = middle
        else:
            right = middle
    return get_hash(left) == hash_


class ReplayError(Exception):
    """An error occurred during the replay of an object"""

    def __init__(self, operation, *, obj_id, exc):
        self.operation = operation
        self.obj_id = hash_to_hex(obj_id)
        self.exc = exc

    def __str__(self):
        return "ReplayError(doing %s, %s, %s)" % (self.operation, self.obj_id, self.exc)


def log_replay_retry(retry_state, sleep=None, last_result=None):
    """Log a retry of the content replayer"""
    try:
        exc = retry_state.outcome.exception()
        attempt_number = retry_state.attempt_number
    except AttributeError:
        # tenacity < 5.0
        exc = last_result.exception()
        attempt_number = retry_state.statistics["attempt_number"]

    logger.debug(
        "Retry operation %(operation)s on %(obj_id)s: %(exc)s",
        {"operation": exc.operation, "obj_id": exc.obj_id, "exc": str(exc.exc)},
    )

    statsd.increment(
        CONTENT_RETRY_METRIC,
        tags={"operation": exc.operation, "attempt": str(attempt_number),},
    )


def log_replay_error(retry_state):
    """Log a replay error to sentry"""
    try:
        exc = retry_state.outcome.exception()
    except AttributeError:
        # tenacity < 5.0
        exc = retry_state.exception()

    with push_scope() as scope:
        scope.set_tag("operation", exc.operation)
        scope.set_extra("obj_id", exc.obj_id)
        capture_exception(exc.exc)

    logger.error(
        "Failed operation %(operation)s on %(obj_id)s after %(retries)s"
        " retries: %(exc)s",
        {
            "obj_id": exc.obj_id,
            "operation": exc.operation,
            "exc": str(exc.exc),
            "retries": retry_state.attempt_number,
        },
    )

    return None


CONTENT_REPLAY_RETRIES = 3

content_replay_retry = retry(
    retry=retry_if_exception_type(ReplayError),
    stop=stop_after_attempt(CONTENT_REPLAY_RETRIES),
    wait=wait_random_exponential(multiplier=1, max=60),
    before_sleep=log_replay_retry,
    retry_error_callback=log_replay_error,
)


@content_replay_retry
def copy_object(obj_id, src, dst):
    hex_obj_id = hash_to_hex(obj_id)
    obj = ""
    try:
        with statsd.timed(CONTENT_DURATION_METRIC, tags={"request": "get"}):
            obj = src.get(obj_id)
            logger.debug("retrieved %(obj_id)s", {"obj_id": hex_obj_id})

        with statsd.timed(CONTENT_DURATION_METRIC, tags={"request": "put"}):
            dst.add(obj, obj_id=obj_id, check_presence=False)
            logger.debug("copied %(obj_id)s", {"obj_id": hex_obj_id})
        statsd.increment(CONTENT_BYTES_METRIC, len(obj))
    except ObjNotFoundError:
        logger.error(
            "Failed to copy %(obj_id)s: object not found", {"obj_id": hex_obj_id}
        )
        raise
    except Exception as exc:
        raise ReplayError("copy", obj_id=obj_id, exc=exc) from None
    return len(obj)


@content_replay_retry
def obj_in_objstorage(obj_id, dst):
    """Check if an object is already in an objstorage, tenaciously"""
    try:
        return obj_id in dst
    except Exception as exc:
        raise ReplayError("in_dst", obj_id=obj_id, exc=exc) from None


def process_replay_objects_content(
    all_objects: Dict[str, List[dict]],
    *,
    src: ObjStorage,
    dst: ObjStorage,
    exclude_fn: Optional[Callable[[dict], bool]] = None,
    check_dst: bool = True,
):
    """
    Takes a list of records from Kafka (see
    :py:func:`swh.journal.client.JournalClient.process`) and copies them
    from the `src` objstorage to the `dst` objstorage, if:

    * `obj['status']` is `'visible'`
    * `exclude_fn(obj)` is `False` (if `exclude_fn` is provided)
    * `obj['sha1'] not in dst` (if `check_dst` is True)

    Args:
        all_objects: Objects passed by the Kafka client. Most importantly,
            `all_objects['content'][*]['sha1']` is the sha1 hash of each
            content.
        src: An object storage (see :py:func:`swh.objstorage.get_objstorage`)
        dst: An object storage (see :py:func:`swh.objstorage.get_objstorage`)
        exclude_fn: Determines whether an object should be copied.
        check_dst: Determines whether we should check the destination
            objstorage before copying.

    Example:

    >>> from swh.objstorage.factory import get_objstorage
    >>> src = get_objstorage('memory')
    >>> dst = get_objstorage('memory')
    >>> id1 = src.add(b'foo bar')
    >>> id2 = src.add(b'baz qux')
    >>> kafka_partitions = {
    ...     'content': [
    ...         {
    ...             'sha1': id1,
    ...             'status': 'visible',
    ...         },
    ...         {
    ...             'sha1': id2,
    ...             'status': 'visible',
    ...         },
    ...     ]
    ... }
    >>> process_replay_objects_content(
    ...     kafka_partitions, src=src, dst=dst,
    ...     exclude_fn=lambda obj: obj['sha1'] == id1)
    >>> id1 in dst
    False
    >>> id2 in dst
    True
    """
    vol = []
    nb_skipped = 0
    nb_failures = 0
    t0 = time()

    for (object_type, objects) in all_objects.items():
        if object_type != "content":
            logger.warning(
                "Received a series of %s, this should not happen", object_type
            )
            continue
        for obj in objects:
            obj_id = obj[ID_HASH_ALGO]
            if obj["status"] != "visible":
                nb_skipped += 1
                logger.debug(
                    "skipped %s (status=%s)", hash_to_hex(obj_id), obj["status"]
                )
                statsd.increment(
                    CONTENT_OPERATIONS_METRIC,
                    tags={"decision": "skipped", "status": obj["status"]},
                )
            elif exclude_fn and exclude_fn(obj):
                nb_skipped += 1
                logger.debug("skipped %s (manually excluded)", hash_to_hex(obj_id))
                statsd.increment(
                    CONTENT_OPERATIONS_METRIC, tags={"decision": "excluded"}
                )
            elif check_dst and obj_in_objstorage(obj_id, dst):
                nb_skipped += 1
                logger.debug("skipped %s (in dst)", hash_to_hex(obj_id))
                statsd.increment(CONTENT_OPERATIONS_METRIC, tags={"decision": "in_dst"})
            else:
                try:
                    copied = copy_object(obj_id, src, dst)
                except ObjNotFoundError:
                    nb_skipped += 1
                    statsd.increment(
                        CONTENT_OPERATIONS_METRIC, tags={"decision": "not_in_src"}
                    )
                else:
                    if copied is None:
                        nb_failures += 1
                        statsd.increment(
                            CONTENT_OPERATIONS_METRIC, tags={"decision": "failed"}
                        )
                    else:
                        vol.append(copied)
                        statsd.increment(
                            CONTENT_OPERATIONS_METRIC, tags={"decision": "copied"}
                        )

    dt = time() - t0
    logger.info(
        "processed %s content objects in %.1fsec "
        "(%.1f obj/sec, %.1fMB/sec) - %d failed - %d skipped",
        len(vol),
        dt,
        len(vol) / dt,
        sum(vol) / 1024 / 1024 / dt,
        nb_failures,
        nb_skipped,
    )

    if notify:
        notify("WATCHDOG=1")
