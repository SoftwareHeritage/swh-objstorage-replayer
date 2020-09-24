# Copyright (C) 2016-2020 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

# WARNING: do not import unnecessary things here to keep cli startup time under
# control
import logging

import click

try:
    from systemd.daemon import notify
except ImportError:
    notify = None

from swh.objstorage.cli import objstorage_cli_group


@objstorage_cli_group.command("replay")
@click.option(
    "--stop-after-objects",
    "-n",
    default=None,
    type=int,
    help="Stop after processing this many objects. Default is to run forever.",
)
@click.option(
    "--exclude-sha1-file",
    default=None,
    type=click.File("rb"),
    help="File containing a sorted array of hashes to be excluded.",
)
@click.option(
    "--check-dst/--no-check-dst",
    default=True,
    help="Check whether the destination contains the object before copying.",
)
@click.pass_context
def content_replay(ctx, stop_after_objects, exclude_sha1_file, check_dst):
    """Fill a destination Object Storage using a journal stream.

    This is typically used for a mirror configuration, by reading a Journal
    and retrieving objects from an existing source ObjStorage.

    There can be several 'replayers' filling a given ObjStorage as long as they
    use the same ``group-id``. You can use the ``KAFKA_GROUP_INSTANCE_ID``
    environment variable to use KIP-345 static group membership.

    This service retrieves object ids to copy from the 'content' topic. It will
    only copy object's content if the object's description in the kafka
    nmessage has the status:visible set.

    ``--exclude-sha1-file`` may be used to exclude some hashes to speed-up the
    replay in case many of the contents are already in the destination
    objstorage. It must contain a concatenation of all (sha1) hashes,
    and it must be sorted.
    This file will not be fully loaded into memory at any given time,
    so it can be arbitrarily large.

    ``--check-dst`` sets whether the replayer should check in the destination
    ObjStorage before copying an object. You can turn that off if you know
    you're copying to an empty ObjStorage.
    """
    import functools
    import mmap

    from swh.journal.client import get_journal_client
    from swh.model.model import SHA1_SIZE
    from swh.objstorage.factory import get_objstorage
    from swh.objstorage.replayer.replay import (
        is_hash_in_bytearray,
        process_replay_objects_content,
    )

    conf = ctx.obj["config"]
    try:
        objstorage_src = get_objstorage(**conf.pop("objstorage"))
    except KeyError:
        ctx.fail("You must have a source objstorage configured in " "your config file.")
    try:
        objstorage_dst = get_objstorage(**conf.pop("objstorage_dst"))
    except KeyError:
        ctx.fail(
            "You must have a destination objstorage configured " "in your config file."
        )

    if exclude_sha1_file:
        map_ = mmap.mmap(exclude_sha1_file.fileno(), 0, prot=mmap.PROT_READ)
        if map_.size() % SHA1_SIZE != 0:
            ctx.fail(
                "--exclude-sha1 must link to a file whose size is an "
                "exact multiple of %d bytes." % SHA1_SIZE
            )
        nb_excluded_hashes = int(map_.size() / SHA1_SIZE)

        def exclude_fn(obj):
            return is_hash_in_bytearray(obj["sha1"], map_, nb_excluded_hashes)

    else:
        exclude_fn = None

    client = get_journal_client(
        "kafka",
        **conf["journal_client"],
        stop_after_objects=stop_after_objects,
        object_types=("content",),
    )
    worker_fn = functools.partial(
        process_replay_objects_content,
        src=objstorage_src,
        dst=objstorage_dst,
        exclude_fn=exclude_fn,
        check_dst=check_dst,
    )

    if notify:
        notify("READY=1")

    try:
        client.process(worker_fn)
    except KeyboardInterrupt:
        ctx.exit(0)
    else:
        print("Done.")
    finally:
        if notify:
            notify("STOPPING=1")
        client.close()


def main():
    logging.basicConfig()
    return objstorage_cli_group(auto_envvar_prefix="SWH_OBJSTORAGE")


if __name__ == "__main__":
    main()
