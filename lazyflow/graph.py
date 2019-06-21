###############################################################################
#   lazyflow: data flow based lazy parallel computation framework
#
#       Copyright (C) 2011-2014, the ilastik developers
#                                <team@ilastik.org>
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the Lesser GNU General Public License
# as published by the Free Software Foundation; either version 2.1
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# See the files LICENSE.lgpl2 and LICENSE.lgpl3 for full text of the
# GNU Lesser General Public License version 2.1 and 3 respectively.
# This information is also available on the ilastik web site at:
# 		   http://ilastik.org/license/
###############################################################################
"""
This module implements the basic flow graph
of the lazyflow module.

Basic usage example:

---
import numpy
import lazyflow.graph
from lazyflow.operators.operators import  OpArrayPiper


g = lazyflow.graph.Graph()

operator1 = OpArrayPiper(graph=g)
operator2 = OpArrayPiper(graph=g)

operator1.inputs["Input"].setValue(numpy.zeros((10,20,30), dtype=numpy.uint8))

operator2.inputs["Input"].connect(operator1.outputs["Output"])

result = operator2.outputs["Output"][:].wait()
---

"""

# Python
import sys
import copy
import functools
import collections
import itertools
import threading
import logging

logger = logging.getLogger(__name__)

# third-party
import psutil

if int(psutil.__version__.split(".")[0]) < 1 and int(psutil.__version__.split(".")[1]) < 3:
    msg = "Lazyflow: Please install a psutil python module version" " of at least >= 0.3.0"
    sys.stderr.write(msg)
    logger.error(msg)
    sys.exit(1)

# SciPy
import numpy

# lazyflow
from lazyflow import rtype
from lazyflow.request import Request
from lazyflow.stype import ArrayLike
from lazyflow.utility import slicingtools, Tracer, OrderedSignal, Singleton
from lazyflow.slot import InputSlot, OutputSlot, Slot
from lazyflow.operator import Operator, InputDict, OutputDict, OperatorMetaClass
from lazyflow.operatorWrapper import OperatorWrapper
from lazyflow.metaDict import MetaDict


class Graph:
    """
    A Graph instance is shared by all connected operators and contains any
    bookkeeping or globally accessible state needed by all operators/slots in the graph.
    """

    class Transaction:
        class State:
            Active = 'Active'
            Exiting = 'Exiting'
            Inactive = 'Inactive'

        def __init__(self, lock, graph):
            self._deferred_callbacks = None
            self._state = self.State.Inactive
            self._lock = lock
            self._graph = graph

        @property
        def active(self):
            return self._state == self.State.Active

        def on_exit(self, fn, operator):
            if self._state == self.State.Active:
                if (fn, operator) in self._deferred_callbacks:
                    return
                else:
                    self._deferred_callbacks.append((fn, operator))
            elif self._state == self.State.Exiting:
                if (fn, operator) in self._deferred_callbacks:
                    return
                else:
                    fn()
            else:
                fn()

        def __enter__(self):
            assert not self.active, "Nested transactions are not supported"
            self._lock.acquire()
            self._state = self.State.Active
            self._deferred_callbacks = []

        def __exit__(self, *args, **kw):
            # On exit we making transaction inactive immidiatelly to avoid adding more deferred callbacks
            self._state = self.State.Exiting
            try:
                ranks = topo_sort(self._graph._ops)
                print("RANKS", ranks)
                for cb, operator in sorted(self._deferred_callbacks, key=lambda val: ranks[val[1]]):
                    cb()
            finally:
                print("EXIT TRANSACTION")
                self._deferred_callbacks = None
                self._state = self.State.Inactive
                self._lock.release()

    def __init__(self):
        self._setup_depth = 0
        self._sig_setup_complete = None
        self._lock = threading.Lock()
        self.rwlock = RWLock()
        self.transaction = self.Transaction(self.rwlock.writer, self)
        self._ops = []

    def register_operator(self, operator):
        self._ops.append(operator)

    def call_when_setup_finished(self, fn):
        # The graph is considered in "setup" mode if any slot is executing a function that affects the state of the graph.
        # See slot.py for details.  Such operations typically invoke a chain reaction of setup operations.
        # The entire setup is "finished" when the initially invoked setup function returns.
        """
        See comment above.

        If the graph is not in the middle of a setup operation as described above,
        immediately call the given callback.  Otherwise, save the callback and
        execute it when the setup operation completes.  The callback is executed
        only once, and then it is discarded.
        """
        if self._setup_depth == 0:
            # Not setting up.  Call immediately
            fn()
        else:
            # Subscribe to the next completion.
            self._sig_setup_complete.subscribe(fn)

    def maybe_call_within_transaction(self, fn, operator=None):
        # Call or not to call should be decided by transaction
        # if we already have deferred function in queue then we'll avoid calling it one more time
        self.transaction.on_exit(fn, operator)

    class SetupDepthContext(object):
        """
        A context manager to manage the "depth" of a setup operation.
        When the depth reaches zero, the graph's `_sig_setup_complete` signal is emitted.
        """

        def __init__(self, g):
            self._graph = g

        def __enter__(self):
            if self._graph:
                with self._graph._lock:
                    if self._graph._setup_depth == 0:
                        self._graph._sig_setup_complete = OrderedSignal()
                    self._graph._setup_depth += 1

        def __exit__(self, *args):
            if self._graph:
                sig_setup_complete = None
                with self._graph._lock:
                    self._graph._setup_depth -= 1
                    if self._graph._setup_depth == 0:
                        sig_setup_complete = self._graph._sig_setup_complete
                        # Reset.
                        self._graph._sig_setup_complete = None
                if sig_setup_complete:
                    sig_setup_complete()


import threading
from contextlib import contextmanager

class RWLock:
    def __init__(self, lock_factory=threading.Lock):
        self._wlock = lock_factory()
        self._rlock = lock_factory()
        self._readers_count = 0

    @property
    @contextmanager
    def reader(self):
        with self._rlock:
            self._readers_count += 1

            if self._readers_count == 1:
                self._wlock.acquire()

        #print(f"READERS: {self._readers_count}")
        yield self

        with self._rlock:
            self._readers_count -= 1

            if self._readers_count == 0:
                self._wlock.release()

    @property
    def writer(self):
        return self._wlock


def iter_slots(op):
    yield from op.inputs.values()
    yield from op.outputs.values()

def downstream_ops(op):
    ops = set()
    for slot in iter_slots(op):
        for s in slot.downstream_slots:
            ops.add(s.operator)

    return ops

def topo_sort(ops):
    stack = []
    visited = {op: False for op in ops}

    def topo_sort_until(op):
        visited[op] = True
        for dop in downstream_ops(op):
            if not visited[dop]:
                topo_sort_until(dop)

        stack.insert(0, op)

    for op in ops:
        if not visited[op]:
            topo_sort_until(op)

    return {
        op: rank for rank, op in enumerate(stack)
    }
