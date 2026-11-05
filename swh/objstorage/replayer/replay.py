# Copyright (C) 2019-2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
from queue import Empty, Queue
from threading import Event, Thread
from time import time
from traceback import format_exception, format_tb
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

from humanize import naturaldelta, naturalsize
import sentry_sdk
import yaml

from swh.model.hashutil import HashDict
from swh.objstorage.interface import ObjStorageInterface, objid_from_dict

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
from swh.objstorage.exc import Error

# import the factory module is needed to make tests work (get_objstorage is patched)
import swh.objstorage.factory as factory

logger = logging.getLogger(__name__)
REPORTER: Optional[Callable[[str, bytes], Any]] = None

CONTENT_OPERATIONS_METRIC = "swh_content_replayer_operations_total"
CONTENT_RETRY_METRIC = "swh_content_replayer_retries_total"
CONTENT_BYTES_METRIC = "swh_content_replayer_bytes"
CONTENT_DURATION_METRIC = "swh_content_replayer_duration_seconds"

DEFAULT_MIN_BATCH_SIZE = 1
DEFAULT_MAX_BATCH_SIZE = 1000


def now():
    return datetime.datetime.now(datetime.UTC)


class LengthMismatch(Exception):
    def __init__(self, expected, received):
        self.expected = expected
        self.received = received

    def __str__(self):
        return f"Length mismatch: received {self.received} != expected {self.expected}"


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


def format_obj_id(obj_id: HashDict) -> str:
    return ";".join(
        (
            "%s:%s" % (algo, hash_to_hex(hash))
            for algo, hash in sorted(obj_id.items())
            if hash
        )
    )


def hex_obj_id(obj_id: HashDict) -> Dict[str, str]:
    return {algo: hash_to_hex(hash) for algo, hash in obj_id.items() if hash}


def logger_debug_obj_id(msg, args, **kwargs):
    if logger.isEnabledFor(logging.DEBUG):
        # Ignore this helper in line/function calculation
        kwargs = {**kwargs, "stacklevel": kwargs.get("stacklevel", 1) + 1}
        args["obj_ids"] = ", ".join(map(format_obj_id, args["obj_ids"]))
        logger.debug(msg, args, **kwargs)


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

    def __init__(
        self,
        *,
        obj_ids: List[HashDict],
        exc,
    ) -> None:
        self.obj_ids = obj_ids
        self.exc = exc

    def __str__(self) -> str:
        return "ReplayError(%s, %r, %s)" % (
            (", ".join(map(format_obj_id, self.obj_ids))),
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
        "Retry operation %(operation)s on %(obj_ids)s: %(exc)s",
        {
            "operation": operation,
            "obj_ids": exc.obj_ids,
            "exc": str(exc.exc),
        },
    )


def log_replay_error(
    obj_ids: List[HashDict],
    exc: Exception,
    operation: str,
    retries: int,
) -> None:
    with sentry_sdk.push_scope() as scope:
        scope.set_tag("operation", operation)
        scope.set_extra(
            "obj_ids",
            list(map(hex_obj_id, obj_ids)),
        )
        sentry_sdk.capture_exception(exc)

    error_context = {
        "obj_ids": list(map(format_obj_id, obj_ids)),
        "operation": operation,
        "exc": format_exception(exc) if exc is not None else None,
        "retries": retries,
    }
    logger.error(
        "Failed operation %(operation)s on %(obj_ids)s after %(retries)s"
        " retries; last exception: %(exc)s",
        error_context,
    )

    # if we have a global error (redis) reporter
    if REPORTER is not None:
        # keep the obj_id as a list of dicts in the redis error report
        error_context["obj_ids"] = obj_ids
        key = f"{now().isoformat()}/blob"
        msg = yaml.dump(error_context).encode()
        REPORTER(key, msg)


def retry_error_callback(retry_state: RetryCallState) -> None:
    """Log a replay error to sentry"""
    assert retry_state.outcome
    exc = retry_state.outcome.exception()

    assert isinstance(exc, ReplayError)
    assert retry_state.fn

    operation = retry_state.fn.__name__

    log_replay_error(
        obj_ids=exc.obj_ids,
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
def get_objects(
    obj_ids: List[HashDict],
    objstorage: ObjStorageInterface,
) -> Tuple[List[Tuple[HashDict, bytes]], List[HashDict]]:
    """Returns pairs of `(obj_id, content)` and the set of objects that were not found."""
    try:
        with statsd.timed(CONTENT_DURATION_METRIC, tags={"request": "get_batch"}):
            objs = list(objstorage.get_batch(obj_ids))
    except Exception as exc:
        raise ReplayError(obj_ids=obj_ids, exc=exc) from None

    logger_debug_obj_id("retrieved %(obj_ids)s", {"obj_ids": obj_ids})

    got_obj_ids = {format_obj_id(obj_id) for (obj_id, _) in objs}
    missing_obj_ids = [
        obj_id for obj_id in obj_ids if format_obj_id(obj_id) not in got_obj_ids
    ]
    return (objs, missing_obj_ids)


def check_hashes(obj: bytes, obj_id: HashDict):
    h = MultiHash.from_data(obj, hash_names=obj_id.keys())
    computed = h.digest()

    if computed != obj_id:
        exc = HashMismatch(obj_id, computed)
        log_replay_error(obj_ids=[obj_id], exc=exc, operation="check_hashes", retries=1)
        raise exc


@content_replay_retry
def put_object(obj_id: HashDict, obj: bytes, objstorage: ObjStorageInterface):
    try:
        logger_debug_obj_id("putting %(obj_ids)s", {"obj_ids": [obj_id]})
        with statsd.timed(CONTENT_DURATION_METRIC, tags={"request": "put"}):
            logger_debug_obj_id("storing %(obj_ids)s", {"obj_ids": [obj_id]})
            objstorage.add(obj, obj_id, check_presence=False)
            logger_debug_obj_id("stored %(obj_ids)s", {"obj_ids": [obj_id]})
    except Exception as exc:
        logger_debug_obj_id(
            "will retry putting %(obj_ids)s, failed with: %(exc)r",
            {"obj_ids": [obj_id], "exc": exc},
        )
        raise ReplayError(obj_ids=[obj_id], exc=exc) from None


@content_replay_retry
def obj_in_objstorage(obj_id: HashDict, dst: ObjStorageInterface) -> bool:
    """Check if an object is already in an objstorage, tenaciously"""
    try:
        return obj_id in dst
    except Exception as exc:
        raise ReplayError(obj_ids=[obj_id], exc=exc) from None


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
        min_batch_size: int = DEFAULT_MIN_BATCH_SIZE,
        max_batch_size: int = DEFAULT_MAX_BATCH_SIZE,
    ):
        """Helper class that takes a list of records from Kafka (see
        :py:func:`swh.journal.client.JournalClient.process`) and copies them
        from the `src` objstorage to the `dst` objstorage, if:

        * `obj['status']` is `'visible'`
        * `exclude_fn(obj)` is `False` (if `exclude_fn` is provided)
        * `HashDict(**obj) not in dst` (if `check_dst` is True)

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
                Replicates in the main thread if 0.
            min_batch_size: How many contents to retrieve and add at the same time.
                Higher values improve load on some objstorage backends, at the expense
                of latency.
            max_batch_size: How many contents to retrieve and add at the same time.
                Higher values improve throughput, at the expense of higher memory usage.

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
        self.min_batch_size = min_batch_size
        self.max_batch_size = max_batch_size
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

    def _copy_objects(
        self,
        objs: List[Dict[str, Any]],
        src: ObjStorageInterface,
        dst: ObjStorageInterface,
    ) -> Iterator[Tuple[str, int]]:
        obj_ids = []
        for obj in objs:
            obj_id = objid_from_dict(obj)
            if not obj_id:
                raise ValueError("Object is missing the keys expected in HashDict", obj)
            obj_ids.append(obj_id)

        logger_debug_obj_id("Starting copy objects %(obj_ids)s", {"obj_ids": obj_ids})

        obj_ids_to_copy = []
        expected_length_by_obj_id = {}  # keyed by format_obj_id(obj_id) to be hashable
        for obj_id, obj in zip(obj_ids, objs):
            formatted_obj_id = format_obj_id(obj_id)
            decision = None
            tags = {}
            if obj["status"] != "visible":
                logger_debug_obj_id(
                    "skipped %(obj_ids)s (status=%(status)s)",
                    {"obj_ids": [obj_id], "status": obj["status"]},
                )
                decision = "skipped"
                tags["status"] = obj["status"]
            elif self.exclude_fn and self.exclude_fn(obj):
                logger_debug_obj_id(
                    "skipped %(obj_ids)s (manually excluded)", {"obj_ids": [obj_id]}
                )
                decision = "excluded"
            elif self.check_dst and obj_in_objstorage(obj_id, dst):
                decision = "in_dst"
                if self.check_obj:
                    try:
                        dst.check(obj_id)
                    except Error:
                        logger.info("invalid object found in dst %s", formatted_obj_id)
                        # TODO: add tags["status"] = "invalid_in_dst" in the loop below

            if decision is not None:
                tags["decision"] = decision
                statsd.increment(CONTENT_OPERATIONS_METRIC, tags=tags)
                yield (decision, 0)
            else:
                obj_ids_to_copy.append(obj_id)
                expected_length_by_obj_id[formatted_obj_id] = obj["length"]

        if not obj_ids_to_copy:
            return

        try:
            found_objs, missing_obj_ids = get_objects(obj_ids_to_copy, src)
        except Exception as exc:
            logger.info("failed %s", format_obj_id(obj_id), exc_info=exc)
            tags["decision"] = decision = "failed"
            statsd.increment(CONTENT_OPERATIONS_METRIC, len(obj_ids_to_copy), tags=tags)
            for _ in obj_ids_to_copy:
                yield (decision, 0)
            return

        # if left is smaller, the main thread will get stuck waiting for results
        # from this thread.
        assert len(found_objs) + len(missing_obj_ids) == len(obj_ids_to_copy)

        for missing_obj_id in missing_obj_ids:
            logger.error(
                "Failed to retrieve %(obj_id)s: object not found",
                {"obj_id": format_obj_id(missing_obj_id)},
            )
            decision = "not_found"
            if not self.check_dst and obj_in_objstorage(obj_id, dst):
                tags["status"] = "found_in_dst"
            yield (decision, 0)

        for obj_id, content in found_objs:
            formatted_obj_id = format_obj_id(obj_id)
            obj_len = expected_length_by_obj_id[formatted_obj_id]
            copied_bytes = 0
            tags = {}
            try:
                if len(content) != obj_len:
                    raise LengthMismatch(obj_len, len(content))

                if self.check_src_hashes:
                    check_hashes(content, obj_id)
                put_object(obj_id, content, dst)
            except LengthMismatch as exc:
                logger.info("length mismatch %s", formatted_obj_id, exc_info=exc)
                decision = "length_mismatch"
                if not self.check_dst and obj_in_objstorage(obj_id, dst):
                    tags["status"] = "found_in_dst"
            except HashMismatch as exc:
                logger.info("hash mismatch %s", formatted_obj_id, exc_info=exc)
                decision = "hash_mismatch"
            except Exception as exc:
                logger.info("failed %s", formatted_obj_id, exc_info=exc)
                decision = "failed"
            else:
                statsd.increment(CONTENT_BYTES_METRIC, len(content))
                copied_bytes = len(content)
                logger_debug_obj_id(
                    "copied %(obj_ids)s (%(bytes)d)",
                    {"obj_ids": [obj_id], "bytes": copied_bytes},
                )
                decision = "copied"
            tags["decision"] = decision
            statsd.increment(CONTENT_OPERATIONS_METRIC, tags=tags)
            yield decision, copied_bytes

    def _worker(self):
        """
        return self._worker_inner()
        """
        try:
            return self._worker_inner()
        except Exception:
            logger.exception("Exception in replayer thread:")
            raise

    def _worker_inner(self):
        src = factory.get_objstorage(**self.src_cfg)
        dst = factory.get_objstorage(**self.dst_cfg)
        while not self.stop_event.is_set():
            objs = []
            for _ in range(self.min_batch_size):
                while not self.stop_event.is_set():
                    try:
                        objs.append(self.obj_queue.get(timeout=1))
                    except Empty:
                        pass
                    else:
                        break
            for _ in range(self.min_batch_size, self.max_batch_size):
                try:
                    objs.append(self.obj_queue.get(timeout=1))
                except Empty:
                    break
                if self.stop_event.is_set():
                    break

            if not objs:
                continue
            for result in self._copy_objects(objs, src=src, dst=dst):
                try:
                    decision, nbytes = result
                except ValueError:
                    assert isinstance(
                        result, Exception
                    ), f"{result!r} is neither a 2-tuple nor an exception"
                    self.return_queue.put(("error", 0, result))
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
                "length_mismatch",
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
                if not self.workers:
                    # configured to replicate from the main thread
                    self._worker()
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
