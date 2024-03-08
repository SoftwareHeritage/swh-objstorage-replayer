# Copyright (C) 2019-2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
from queue import Empty, Queue
import sys
from threading import Event, Thread
from time import time
from traceback import format_tb
from typing import Any, Callable, Dict, List, Optional, Tuple

from humanize import naturaldelta, naturalsize
import msgpack
import sentry_sdk

from swh.objstorage.interface import (
    CompositeObjId,
    ObjStorageInterface,
    objid_from_dict,
)

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_random_exponential,
)
from tenacity.retry import retry_base

from swh.core.statsd import statsd
from swh.model.hashutil import MultiHash, hash_to_hex
from swh.model.model import SHA1_SIZE
from swh.objstorage.exc import Error, ObjNotFoundError

# import the factory module is needed to make tests work (get_objstorage is patched)
import swh.objstorage.factory as factory

logger = logging.getLogger(__name__)
REPORTER: Optional[Callable[[str, bytes], Any]] = None

CONTENT_OPERATIONS_METRIC = "swh_content_replayer_operations_total"
CONTENT_RETRY_METRIC = "swh_content_replayer_retries_total"
CONTENT_BYTES_METRIC = "swh_content_replayer_bytes"
CONTENT_DURATION_METRIC = "swh_content_replayer_duration_seconds"


class HashMismatch(Exception):
    def __init__(self, expected, received):
        self.mismatched = {}
        self.matched = {}
        for algo, value in expected.items():
            received_value = received.get(algo)
            if received_value != value:
                self.mismatched[algo] = (received_value, value)
            else:
                self.matched[algo] = value

    def __str__(self):
        return "\n".join(
            ["Hash Mismatch:"]
            + [
                f"  {algo}: {v[0].hex()} != expected {v[1].hex()}"
                for algo, v in self.mismatched.items()
            ]
            + (
                ["Matched hashes:"]
                + [f" {algo}: {v.hex()}" for algo, v in self.matched]
            )
            if self.matched
            else []
        )


def format_obj_id(obj_id: CompositeObjId) -> str:
    return ";".join(
        (
            "%s:%s" % (algo, hash_to_hex(hash))
            for algo, hash in sorted(obj_id.items())
            if hash
        )
    )


def hex_obj_id(obj_id: CompositeObjId) -> Dict[str, str]:
    return {algo: hash_to_hex(hash) for algo, hash in obj_id.items() if hash}


def logger_debug_obj_id(msg, args, **kwargs):
    if logger.isEnabledFor(logging.DEBUG):
        if sys.version_info >= (3, 8):
            # Ignore this helper in line/function calculation
            kwargs = {**kwargs, "stacklevel": kwargs.get("stacklevel", 1) + 1}
        logger.debug(msg, {**args, "obj_id": format_obj_id(args["obj_id"])}, **kwargs)


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

    def __init__(self, *, obj_id: CompositeObjId, exc) -> None:
        self.obj_id = obj_id
        self.exc = exc

    def __str__(self) -> str:
        return "ReplayError(%s, %r, %s)" % (
            format_obj_id(self.obj_id),
            self.exc,
            format_tb(self.exc.__traceback__),
        )


def log_replay_retry(
    retry_state: RetryCallState, sleep: Optional[float] = None, last_result: Any = None
) -> None:
    """Log a retry of the content replayer"""
    assert retry_state.outcome is not None
    exc = retry_state.outcome.exception()
    assert isinstance(exc, ReplayError)
    assert retry_state.fn is not None
    operation = retry_state.fn.__name__
    logger_debug_obj_id(
        "Retry operation %(operation)s on %(obj_id)s: %(exc)s",
        {
            "operation": operation,
            "obj_id": exc.obj_id,
            "exc": str(exc.exc),
        },
    )


def log_replay_error(
    obj_id: CompositeObjId, exc: Exception, operation: str, retries: int
) -> None:
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("operation", operation)
        scope.set_extra("obj_id", hex_obj_id(obj_id))
        sentry_sdk.capture_exception(exc)

    error_context = {
        "obj_id": format_obj_id(obj_id),
        "operation": operation,
        "exc": str(exc),
        "retries": retries,
    }

    logger.error(
        "Failed operation %(operation)s on %(obj_id)s after %(retries)s"
        " retries; last exception: %(exc)s",
        error_context,
    )

    # if we have a global error (redis) reporter
    if REPORTER is not None:
        oid = f"blob:{format_obj_id(obj_id)}"
        msg = msgpack.dumps(error_context)
        REPORTER(oid, msg)


def retry_error_callback(retry_state: RetryCallState) -> None:
    """Log a replay error to sentry"""
    assert retry_state.outcome
    exc = retry_state.outcome.exception()

    assert isinstance(exc, ReplayError)
    assert retry_state.fn

    operation = retry_state.fn.__name__

    log_replay_error(
        obj_id=exc.obj_id,
        exc=exc.exc,
        operation=operation,
        retries=retry_state.attempt_number,
    )

    raise exc


CONTENT_REPLAY_RETRIES = 3


class retry_log_if_success(retry_base):
    """Log in statsd the number of attempts required to succeed"""

    def __call__(self, retry_state: RetryCallState):
        assert retry_state.outcome
        if not retry_state.outcome.failed:
            assert retry_state.fn
            statsd.increment(
                CONTENT_RETRY_METRIC,
                tags={
                    "operation": retry_state.fn.__name__,
                    "attempt": str(retry_state.attempt_number),
                },
            )
        return False


content_replay_retry = retry(
    retry=retry_if_exception_type(ReplayError) | retry_log_if_success(),
    stop=stop_after_attempt(CONTENT_REPLAY_RETRIES),
    wait=wait_random_exponential(multiplier=1, max=60),
    before_sleep=log_replay_retry,
    retry_error_callback=retry_error_callback,
)


@content_replay_retry
def get_object(objstorage: ObjStorageInterface, obj_id: CompositeObjId) -> bytes:
    try:
        with statsd.timed(CONTENT_DURATION_METRIC, tags={"request": "get"}):
            obj = objstorage.get(obj_id)
            logger_debug_obj_id("retrieved %(obj_id)s", {"obj_id": obj_id})
        return obj
    except ObjNotFoundError:
        logger.error(
            "Failed to retrieve %(obj_id)s: object not found",
            {"obj_id": format_obj_id(obj_id)},
        )
        raise
    except Exception as exc:
        raise ReplayError(obj_id=obj_id, exc=exc) from None


def check_hashes(obj: bytes, obj_id: CompositeObjId):
    h = MultiHash.from_data(obj, hash_names=obj_id.keys())
    computed = h.digest()

    if computed != obj_id:
        exc = HashMismatch(obj_id, computed)
        log_replay_error(obj_id=obj_id, exc=exc, operation="check_hashes", retries=1)
        raise exc


@content_replay_retry
def put_object(objstorage: ObjStorageInterface, obj_id: CompositeObjId, obj: bytes):
    try:
        logger_debug_obj_id("putting %(obj_id)s", {"obj_id": obj_id})
        with statsd.timed(CONTENT_DURATION_METRIC, tags={"request": "put"}):
            logger_debug_obj_id("storing %(obj_id)s", {"obj_id": obj_id})
            objstorage.add(obj, obj_id, check_presence=False)
            logger_debug_obj_id("stored %(obj_id)s", {"obj_id": obj_id})
    except Exception as exc:
        logger.error(
            "putting %(obj_id)s failed: %(exc)r",
            {"obj_id": format_obj_id(obj_id), "exc": exc},
        )
        raise ReplayError(obj_id=obj_id, exc=exc) from None


def copy_object(
    obj_id: CompositeObjId,
    src: ObjStorageInterface,
    dst: ObjStorageInterface,
    check_src_hashes: bool = False,
) -> int:
    obj = get_object(src, obj_id)
    if obj is not None:
        if check_src_hashes:
            check_hashes(obj, obj_id)
        put_object(dst, obj_id, obj)
        statsd.increment(CONTENT_BYTES_METRIC, len(obj))
        return len(obj)
    return 0


@content_replay_retry
def obj_in_objstorage(obj_id: CompositeObjId, dst: ObjStorageInterface) -> bool:
    """Check if an object is already in an objstorage, tenaciously"""
    try:
        return obj_id in dst
    except Exception as exc:
        raise ReplayError(obj_id=obj_id, exc=exc) from None


class ContentReplayer:
    def __init__(
        self,
        src: Dict[str, Any],
        dst: Dict[str, Any],
        exclude_fn: Optional[Callable[[Dict[str, Any]], bool]] = None,
        check_dst: bool = True,
        check_obj: bool = False,
        check_src_hashes: bool = False,
        concurrency: int = 16,
    ):
        """Helper class that takes a list of records from Kafka (see
        :py:func:`swh.journal.client.JournalClient.process`) and copies them
        from the `src` objstorage to the `dst` objstorage, if:

        * `obj['status']` is `'visible'`
        * `exclude_fn(obj)` is `False` (if `exclude_fn` is provided)
        * `CompositeObjId(**obj) not in dst` (if `check_dst` is True)

        Args:
            src: An object storage configuration dict (see
                :py:func:`swh.objstorage.get_objstorage`)
            dst: An object storage configuration dict (see
                :py:func:`swh.objstorage.get_objstorage`)
            exclude_fn: Determines whether an object should be copied.
            check_dst: Determines whether we should check the destination
                objstorage before copying.
            check_obj: If check_dst is true, determines whether we should check
                the existing object in the destination objstorage is valid; if not,
                put the replayed object.
            check_src_hashes: Checks the object before sending it to the dst objstorage.
            concurrency: Number of worker threads doing the replication process
                (retrieve, check, store).

        See swh/objstorage/replayer/tests/test_replay.py for usage examples.
        """
        self.src_cfg = src
        self.dst_cfg = dst
        self.exclude_fn = exclude_fn
        self.check_dst = check_dst
        self.check_obj = check_obj
        self.check_src_hashes = check_src_hashes
        self.concurrency = concurrency
        self.obj_queue: Queue = Queue()
        self.return_queue: Queue = Queue()
        self.stop_event = Event()
        self.workers = [Thread(target=self._worker) for i in range(self.concurrency)]
        for w in self.workers:
            w.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def stop(self):
        """Stop replayer's worker threads"""
        self.stop_event.set()
        for worker in self.workers:
            worker.join()

    def _copy_object(
        self, obj: Dict[str, Any], src: ObjStorageInterface, dst: ObjStorageInterface
    ):
        obj_id = objid_from_dict(obj)

        if not obj_id:
            raise ValueError(
                "Object is missing the keys expected in CompositeObjId", obj
            )

        logger_debug_obj_id("Starting copy object %(obj_id)s", {"obj_id": obj_id})
        decision = None
        copied_bytes = 0
        tags = {}

        if obj["status"] != "visible":
            logger_debug_obj_id(
                "skipped %(obj_id)s (status=%(status)s)",
                {"obj_id": obj_id, "status": obj["status"]},
            )
            decision = "skipped"
            tags["status"] = obj["status"]
        elif self.exclude_fn and self.exclude_fn(obj):
            logger_debug_obj_id(
                "skipped %(obj_id)s (manually excluded)", {"obj_id": obj_id}
            )
            decision = "excluded"
        elif self.check_dst and obj_in_objstorage(obj_id, dst):
            decision = "in_dst"
            if self.check_obj:
                try:
                    dst.check(obj_id)
                except Error:
                    logger.info("invalid object found in dst %s", format_obj_id(obj_id))
                    decision = None
                    tags["status"] = "invalid_in_dst"
        if decision is None:
            try:
                copied_bytes = copy_object(
                    obj_id, src=src, dst=dst, check_src_hashes=self.check_src_hashes
                )
            except ObjNotFoundError:
                logger_debug_obj_id("not found %(obj_id)s", {"obj_id": obj_id})
                decision = "not_found"
                if not self.check_dst and obj_in_objstorage(obj_id, dst):
                    tags["status"] = "found_in_dst"
            except HashMismatch as exc:
                logger.info("hash mismatch %s", format_obj_id(obj_id), exc_info=exc)
                decision = "hash_mismatch"
            except Exception as exc:
                logger.info("failed %s", format_obj_id(obj_id), exc_info=exc)
                decision = "failed"
            else:
                if copied_bytes is None:
                    logger_debug_obj_id("failed %(obj_id)s (None)", {"obj_id": obj_id})
                    decision = "failed"
                else:
                    logger_debug_obj_id(
                        "copied %(obj_id)s (%(bytes)d)",
                        {"obj_id": obj_id, "bytes": copied_bytes},
                    )
                    decision = "copied"
        tags["decision"] = decision
        statsd.increment(CONTENT_OPERATIONS_METRIC, tags=tags)
        return decision, copied_bytes

    def _worker(self):
        src = factory.get_objstorage(**self.src_cfg)
        dst = factory.get_objstorage(**self.dst_cfg)
        while not self.stop_event.is_set():
            try:
                obj = self.obj_queue.get(timeout=1)
            except Empty:
                continue
            try:
                decision, nbytes = self._copy_object(obj, src=src, dst=dst)
            except Exception as exc:
                self.return_queue.put(("error", 0, exc))
            else:
                self.return_queue.put((decision, nbytes, None))

    def replay(
        self,
        all_objects: Dict[str, List[dict]],
    ):
        vol = 0
        stats = dict.fromkeys(
            [
                "skipped",
                "excluded",
                "not_found",
                "failed",
                "copied",
                "in_dst",
                "hash_mismatch",
            ],
            0,
        )
        t0 = time()
        nobjs = 0
        for object_type, objects in all_objects.items():
            if object_type != "content":
                logger.warning(
                    "Received a series of %s, this should not happen", object_type
                )
                continue
            for obj in objects:
                self.obj_queue.put(obj)
                nobjs += 1

        logger.debug("Waiting for the obj queue to be processed")
        results: List[Tuple[str, int, Optional[Exception]]] = []
        while (not self.stop_event.is_set()) and (len(results) < nobjs):
            try:
                result = self.return_queue.get(timeout=1)
            except Empty:
                continue
            else:
                results.append(result)

        logger.debug("Checking results")
        for decision, nbytes, exc in results:
            if exc:
                # XXX this should not happen, so it is probably wrong...
                raise exc
            else:
                if nbytes is not None:
                    vol += nbytes
                stats[decision] += 1

        dt = time() - t0
        logger.info(
            "processed %s content objects (%s) in %s "
            "(%.1f obj/sec, %s/sec) "
            "- %d copied - %d in dst - %d skipped "
            "- %d excluded - %d not found - %d failed",
            nobjs,
            naturalsize(vol),
            naturaldelta(dt),
            nobjs / dt,
            naturalsize(vol / dt),
            stats["copied"],
            stats["in_dst"],
            stats["skipped"],
            stats["excluded"],
            stats["not_found"],
            stats["failed"],
        )

        if notify:
            notify("WATCHDOG=1")
