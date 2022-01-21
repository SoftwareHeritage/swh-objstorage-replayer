.. _swh-objstorage-replayer:

Software Heritage - Object storage replayer
===========================================

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
typically useful when the replication of one object comes with non negligeable
minimal latency (e.g. consuming from public cloud-based objstorages).


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   cli
   /apidoc/swh.objstorage.replayer
