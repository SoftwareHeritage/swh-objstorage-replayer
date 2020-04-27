.. _swh-objstorage-replayer:

Software Heritage - Object storage replayer
===========================================

This Python module provides a command line tool to replicate content objects from a
source Object storage to a destination one by listening the `content` topic of a
`swh.journal` kafka stream.

It is meant to be used as the brick of a mirror setup dedicated to replicating content
objects.


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   /apidoc/swh.objstorage.replayer
