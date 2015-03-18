# -*- coding: utf-8 -*-
import struct
import msgpack
import threading

import tarantool


def unpack_long_long(value):
    return struct.unpack("<q", value)[0]


def unpack_long(value):
    return struct.unpack("<l", value)[0]


class TTask(object):
    """
    Tarantool queue task wrapper.

    .. warning::

        Don't instantiate it with your bare hands
    """
    def __init__(self, queue, task_id=0,
                 tube="", raw_data=None):
        self.task_id = unpack_long_long(task_id)
        self.tube = tube
        self.raw_data = raw_data
        self.queue = queue
        self.modified = False

    def ack(self):
        """
        Confirm completion of a task. Before marking a task as complete

        :rtype: `Task` instance
        """
        self.modified = True
        return self.queue._ack(self.task_id)

    def release(self, **kwargs):
        """
        Return a task back to the queue: the task is not executed.

        :param ttl: new time to live
        :param delay: new delay for task
        :type ttl: int
        :type delay: int
        :rtype: `Task` instance
        """
        self.modified = True
        return self.queue._release(self.task_id, **kwargs)

    def delete(self):
        """
        Delete a task from the queue (regardless of task state or status).

        :rtype: boolean
        """
        self.modified = True
        return self.queue._delete(self.task_id)

    @property
    def data(self):
        if not self.raw_data:
            return None
        if not hasattr(self, '_decoded_data'):
            data = self.queue.tube(self.tube).deserialize(self.raw_data)
            self._decoded_data = data
        return self._decoded_data

    def __str__(self):
        args = (
            self.task_id, self.tube, self.queue.space
        )
        return "Task (id: {0}, tube:{1}, space:{2})".format(*args)

    def __del__(self):
        if not self.modified:
            self.release()

    @classmethod
    def from_tuple(cls, queue, the_tuple):
        if the_tuple is None:
            return
        if the_tuple.rowcount < 1:
            raise TQueue.ZeroTupleException('error creating task')
        row = the_tuple[0]
        if len(row) < 9:
            raise TQueue.NoDataException('no data in the task')
        return cls(
            queue,
            task_id=row[0],
            tube=row[4],
            raw_data=row[8],
        )


class TTube(object):
    """
    Tarantol queue tube wrapper. Pinned to space and tube, but unlike TQueue
    it has predefined delay, ttl, ttr, and pri.

    .. warning::

        Don't instantiate it with your bare hands
    """
    def __init__(self, queue, name, **kwargs):
        self.queue = queue
        self.tube = name
        self.opt = {
            'delay': 0,
            'limits': 500000,
            'ttl': 0,
            'ttr': 300,
            'pri': 0x7fff,
            'retry': 5,
            'tube': name
        }
        self.opt.update(kwargs)
        self._serialize = None
        self._deserialize = None

    # ----------------
    @property
    def serialize(self):
        """
        Serialize function: must be Callable or None. Sets None when deleted
        """
        if self._serialize is None:
            return self.queue.serialize
        return self._serialize

    @serialize.setter
    def serialize(self, func):
        if not (hasattr(func, '__call__') or func is None):
            raise TypeError("func must be Callable "
                            "or None, but not " + str(type(func)))
        self._serialize = func

    # ----------------
    @property
    def deserialize(self):
        """
        Deserialize function: must be Callable or None. Sets None when deleted
        """
        if self._deserialize is None:
            return self.queue.deserialize
        return self._deserialize

    @deserialize.setter
    def deserialize(self, func):
        if not (hasattr(func, '__call__') or func is None):
            raise TypeError("func must be Callable "
                            "or None, but not " + str(type(func)))
        self._deserialize = func

    # ----------------
    def update_options(self, **kwargs):
        """
        Update options for current tube (such as ttl, ttr, pri and delay)
        """
        self.opt.update(kwargs)

    def put(self, data, **kwargs):
        """
        Enqueue a task. Returns a tuple, representing the new task.
        The list of fields with task data ('...') is optional.
        If urgent set to True then the task will get the highest priority.
        Default Priority is 0x7ff. Lesser value - more priority.

        :param data: Data for pushing into queue
        :param delay: new delay for task
                      (Not necessary, Default of Tube object)
        :param ttl: new time to live (Not necessary, Default of Tube object)
        :param ttr: time to release (Not necessary, Default of Tube object)
        :param tube: name of Tube (Not necessary, Default of Tube object)
        :param pri: priority (Not necessary, Default of Tube object)
        :param retry: Number of retries (Not necessary, Default of Tube object)
        :param limits: Number of tasks in Tube max
                       (Not necessary, Default of Tube object)
        :type ttl: int
        :type delay: int
        :type ttr: int
        :type tube: string
        :rtype: int
        """
        opt = dict(self.opt, **kwargs)

        method = "box.queue.put"

        the_tuple = self.queue.tnt.call(method, (
            str(self.queue.space),
            str(opt["tube"]),
            str(opt["limits"]),
            str(opt["pri"]),
            str(opt["delay"]),
            str(opt["ttr"]),
            str(opt["ttl"]),
            str(opt["retry"]),
            self.serialize(data))
        )
        return unpack_long_long(the_tuple[0][0])

    def take(self, timeout=0):
        """
        If there are tasks in the queue ready for execution,
        take the highest-priority task. Otherwise, wait for a
        ready task to appear in the queue, and, as soon as it appears,
        mark it as taken and return to the consumer. If there is a
        timeout, and the task doesn't appear until the timeout expires,
        return 'None'. If timeout is None, wait indefinitely until
        a task appears.

        :param timeout: timeout to wait.
        :type timeout: int or None
        :rtype: `Task` instance or None
        """
        return self.queue._take(self.opt['tube'], timeout)


class TQueue(object):
    """
    Tarantool queue wrapper. Surely pinned to space. May create tubes.
    By default it uses msgpack for serialization, but you may redefine
    serialize and deserialize methods.
    You must use TQueue only for creating Tubes.
    For more usage, please, look into tests.
    Usage:

        >>> from tarantool_queue import TQueue
        >>> queue = TQueue()
        >>> tube1 = queue.create_tube('holy_grail', ttl=100, delay=5)
        # Put task into the queue
        >>> tube1.put([1, 2, 3])
        # Put task into the beggining of queue (Highest priority)
        >>> tube1.urgent([2, 3, 4])
        >>> tube1.get() # We get task and automaticaly release it
        >>> task1 = tube1.take()
        >>> task2 = tube1.take()
        >>> print(task1.data)
            [2, 3, 4]
        >>> print(task2.data)
            [1, 2, 3]
        >>> del task2
        >>> del task1
        >>> print(tube1.take().data)
            [1, 2, 3]
        # Take task and Ack it
        >>> tube1.take().ack()
            True
    """

    DataBaseError = tarantool.DatabaseError
    NetworkError = tarantool.NetworkError

    class BadConfigException(Exception):
        pass

    class ZeroTupleException(Exception):
        pass

    class NoDataException(Exception):
        pass

    @staticmethod
    def basic_serialize(data):
        return msgpack.packb(data)

    @staticmethod
    def basic_deserialize(data):
        return msgpack.unpackb(data)

    def __init__(self, host="localhost", port=33013, space=0, schema=None):
        if not(host and port):
            raise TQueue.BadConfigException(
                "host and port params must be not empty")

        if not isinstance(port, int):
            raise TQueue.BadConfigException("port must be int")

        if not isinstance(space, int):
            raise TQueue.BadConfigException("space must be int")

        self.host = host
        self.port = port
        self.space = space
        self.schema = schema
        self.tubes = {}
        self._serialize = self.basic_serialize
        self._deserialize = self.basic_deserialize

    # ----------------
    @property
    def serialize(self):
        """
        Serialize function: must be Callable. If sets to None or deleted, then
        it will use msgpack for serializing.
        """
        if not hasattr(self, '_serialize'):
            self.serialize = self.basic_serialize
        return self._serialize

    @serialize.setter
    def serialize(self, func):
        if not (hasattr(func, '__call__') or func is None):
            raise TypeError("func must be Callable "
                            "or None, but not " + str(type(func)))
        self._serialize = func if func is not None else self.basic_serialize

    @serialize.deleter
    def serialize(self):
        self._serialize = self.basic_serialize

    # ----------------
    @property
    def deserialize(self):
        """
        Deserialize function: must be Callable. If sets to None or delete,
        then it will use msgpack for deserializing.
        """
        if not hasattr(self, '_deserialize'):
            self._deserialize = self.basic_deserialize
        return self._deserialize

    @deserialize.setter
    def deserialize(self, func):
        if not (hasattr(func, '__call__') or func is None):
            raise TypeError("func must be Callable "
                            "or None, but not " + str(type(func)))
        self._deserialize = (func
                             if func is not None
                             else self.basic_deserialize)

    @deserialize.deleter
    def deserialize(self):
        self._deserialize = self.basic_deserialize

    # ----------------
    @property
    def tarantool_connection(self):
        """
        Tarantool Connection class: must be class with methods call and
        __init__. If it sets to None or deleted - it will use the default
        tarantool.Connection class for connection.
        """
        if not hasattr(self, '_conclass'):
            self._conclass = tarantool.Connection
        return self._conclass

    @tarantool_connection.setter
    def tarantool_connection(self, cls):
        if 'call' not in dir(cls) or '__init__' not in dir(cls):
            if cls is not None:
                raise TypeError("Connection class must have"
                                " connect and call methods or be None")
        self._conclass = cls if cls is not None else tarantool.Connection
        if hasattr(self, '_tnt'):
            self.__dict__.pop('_tnt')

    @tarantool_connection.deleter
    def tarantool_connection(self):
        if hasattr(self, '_conclass'):
            self.__dict__.pop('_conclass')
        if hasattr(self, '_tnt'):
            self.__dict__.pop('_tnt')

    # ----------------
    @property
    def tarantool_lock(self):
        """
        Locking class: must be locking instance with methods __enter__
        and __exit__. If it sets to None or delete - it will use default
        threading.Lock() instance for locking in the connecting.
        """
        if not hasattr(self, '_lockinst'):
            self._lockinst = threading.Lock()
        return self._lockinst

    @tarantool_lock.setter
    def tarantool_lock(self, lock):
        if '__enter__' not in dir(lock) or '__exit__' not in dir(lock):
            if lock is not None:
                raise TypeError("Lock class must have `__enter__`"
                                " and `__exit__` methods or be None")
        self._lockinst = lock if lock is not None else threading.Lock()

    @tarantool_lock.deleter
    def tarantool_lock(self):
        if hasattr(self, '_lockinst'):
            self.__dict__.pop('_lockinst')

    # ----------------
    @property
    def tnt(self):
        if not hasattr(self, '_tnt'):
            with self.tarantool_lock:
                if not hasattr(self, '_tnt'):
                    self._tnt = self.tarantool_connection(self.host, self.port,
                                                          schema=self.schema)
        return self._tnt

    def _take(self, tube, timeout=0):
        args = [str(self.space), str(tube)]
        if timeout is not None:
            args.append(str(timeout))
        the_tuple = self.tnt.call("box.queue.take", tuple(args))
        if the_tuple.rowcount == 0:
            return None
        return TTask.from_tuple(self, the_tuple)

    def _ack(self, task_id):
        args = (str(self.space), str(task_id))
        the_tuple = self.tnt.call("box.queue.ack", args)
        return the_tuple.return_code == 0

    def _release(self, task_id, prio=0x7fff, delay=0, ttr=300, ttl=0, retry=5):
        the_tuple = self.tnt.call("box.queue.release", (
            str(self.space),
            str(task_id),
        ))
        return TTask.from_tuple(self, the_tuple)

    def _delete(self, task_id):
        args = (str(self.space), str(task_id))
        the_tuple = self.tnt.call("box.queue.delete", args)
        return the_tuple.return_code == 0

    def tube(self, name, **kwargs):
        """
        Create Tube object, if not created before, and set kwargs.
        If existed, return existed Tube.

        :param name: name of Tube
        :param delay: default delay for Tube tasks (Not necessary, will be 0)
        :param ttl: default TTL for Tube tasks (Not necessary, will be 0)
        :param ttr: default TTR for Tube tasks (Not necessary, will be 0)
        :param pri: default priority for Tube tasks (Not necessary)
        :type name: string
        :type ttl: int
        :type delay: int
        :type ttr: int
        :type pri: int
        :rtype: `Tube` instance
        """
        if name in self.tubes:
            tube = self.tubes[name]
            tube.update_options(**kwargs)
        else:
            tube = TTube(self, name, **kwargs)
            self.tubes[name] = tube
        return tube
