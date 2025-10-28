.. _swh-objstorage-replayer:

.. include:: README.rst


This Python module provides a command line tool to replicate content objects from a
source Object storage to a destination one by listening the ``content`` topic of a
:ref:`swh-journal` kafka stream.

It is meant to be used as the brick of a mirror setup dedicated to replicating content
objects.


Quick start
-----------

Once installed (using pip or debian packages), the command ``swh objstorage
replay`` should be available:

It needs a configuration file with 4 sections:

- ``objstorage``: the source objstorage to retrieve objects from,

- ``objstorage_dst``: the destination objstorage to put objects into,

- ``journal_client``: the journal client (kafka configuration where the object
  hashes are consumed from),

- ``replayer`` (optional): some replayer specific configurations options.


For example with a configuration file like:

.. code-block:: yaml

   objstorage:
     cls: multiplexer
     objstorages:
       - cls: http
         url: https://softwareheritage.s3.amazonaws.com/content/
         compression: gzip
       - cls: remote
         url: https://login:password@objstorage.staging.swh.network

   objstorage_dst:
     cls: remote
     args:
       url: http://objstorage:5003

   journal_client:
     cls: kafka
     brokers:
     - broker1.journal.staging.swh.network:9093
     group_id: kafka-username-content-replayer-003
     sasl.username: kafka-username
     sasl.password: kafka-password
     security.protocol: sasl_ssl
     sasl.mechanism: SCRAM-SHA-512
     session.timeout.ms: 600000
     max.poll.interval.ms: 3600000
     message.max.bytes: 1000000000
     privileged: true
     batch_size: 2000

   replayer:
     error_reporter:
       host: redis
       port: 6379
       db: 0


you can start the content replayer with:

.. code-block:: bash

   $ swh objstorage -C replayer-config.yml replay


You would typically run this tool on several machines, using the same
``group_id``, to increase replication parallelism.

Also note that you may increase the default concurrency within one replayer
using the ``--concurrency`` command line option. This will use as many
replication threads as given in argument, distributing the replication of
objects **within the same kafka consumer** among these threads. This is
typically useful when the replication of one object comes with non negligible
minimal latency (e.g. consuming from public cloud-based objstorages).

Error reporting
---------------

Objects replication may fail. This can happen for several reasons, but we do
not want the replication process to stay stuck when this happens in case of a
sporadic errors. When this happens, the replication process will log the issue
and continue replicating objects.

Errors will be reported in the logging system, however this is not always the
best way to keep track of these specific errors.

So there is an error reporting mechanism provided by the replayer, typically
using a Redis_ key/value DB to collect errors.

This is enabled by configuring the ``replayer/error_reporter`` section in the
configuration file.

Each time an object failed to be replicated from the source objstorage to the
destination one, the event will be logged in Redis using the following format:

- the key is formatted like ``<timestamp>/blob`` where ``timestamp`` is the
  timestamp, as an ISO date format, when the failure occurred

  example::

    2025-10-28T11:27:53.754796+00:00/blob

- the value will be a yaml-encoded dictionary with 4 keys:

  - ``obj_id``: the object keys as a dict of hashes,
  - ``operation``: the name of the function that failed (``get_object`` or ``put_object``)
  - ``retries``: number of retries has been attempted
  - ``exc``: a list of strings representing the exception at the root of the
    failure, if any (None otherwise),


Legacy error reporting formats
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Other error reporting formats have been in use in previous versions of the
``swh-objstorage-replayer`` package, so you may find these formats in an
existing Redis database.

For ``swh-objstorage-replayer  < 2.2`` the format was:

- the key is formatted like ``blob:<hashes>`` where ``hashes`` is the
  identifier of the object as the list of ``{hash_algo}:{hash}`` of the
  object separated by semicolons like::

    blob:blake2s256:d07c3a320456a17bddc0cf064f1bbebed3962c41c73c2667f41c26348278f9e0;\
    sha1:7202f5783c1addfec40f9ec097594200f451bb62;sha1_git:b833f53158793137e81deb5d6d53b9353d0c7393;\
    sha256:78554ca0f037f431117359a83b58dbb1479eb68b019a22aa75ff4e282256261c

- the value is the same dict structure as above but ``msgpack`` serialized and with:
  - ``obj_id``: the object hashes formatted the same as the key

For ``swh-objstorage-replayer < 2.0``:

- the key only contains the ``sha1`` of the object, like::

    blob:7202f5783c1addfec40f9ec097594200f451bb62

- the value is the same dict structure as above but ``msgpack`` serialized and with:
  - ``obj_id``: the sha1 of the object (hex form)



Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   cli

.. only:: standalone_package_doc

   Indices and tables
   ------------------

   * :ref:`genindex`
   * :ref:`modindex`
   * :ref:`search`


.. _Redis: https://redis.io/
