# -*- coding: utf-8 -*-
import re
import struct
import msgpack
import threading

import tarantool


def unpack_long_long(value):
    return struct.unpack("<q", value)[0]


def unpack_long(value):
    return struct.unpack("<l", value)[0]


class Task(object):
    """
    Tarantool queue task wrapper.

    .. warning::

        Don't instantiate it with your bare hands
    """
    def __init__(self, queue, space=0, task_id=0,
                 tube="", status="", raw_data=None):
        self.task_id = task_id
        self.tube = tube
        self.status = status
        self.raw_data = raw_data
        self.space = space
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

    def requeue(self):
        """
        Return a task to the queue, the task is not executed.
        Puts the task at the end of the queue, so that it's
        executed only after all existing tasks in the queue are
        executed.

        :rtype: boolean
        """
        self.modified = True
        return self.queue._requeue(self.task_id)

    def done(self, data):
        """
        Mark a task as complete (done), but don't delete it.
        Replaces task data with the supplied data.

        :param data: Data for pushing into queue
        :rtype: boolean
        """
        self.modified = True
        the_tuple = self.queue.tnt.call("queue.done", (
            str(self.queue.space),
            str(self.task_id),
            self.queue.tube(self.tube).serialize(data))
        )
        return the_tuple.return_code == 0

    def bury(self):
        """
        Mark a task as buried. This special status excludes the
        task from the active list, until it's dug up. This function
        is useful when several attempts to execute a task lead to a
        failure. Buried tasks can be monitored by the queue owner,
        and treated specially.

        :rtype: boolean
        """
        self.modified = True
        return self.queue._bury(self.task_id)

    def dig(self):
        """
        'Dig up' a buried task, after checking that the task is buried.
        The task status is changed to ready.'

        :rtype: boolean
        """
        self.modified = True
        return self.queue._dig(self.task_id)

    def meta(self):
        """
        Return unpacked task metadata.
        :rtype: dict with metainformation or None
        """
        return self.queue._meta(self.task_id)

    def touch(self):
        """
        Prolong living time for taken task with this id.

        :rtype: boolean
        """
        return self.queue._touch(self.task_id)

    @property
    def data(self):
        if not self.raw_data:
            return None
        if not hasattr(self, '_decoded_data'):
            self._decoded_data = self.queue.tube(self.tube).deserialize(self.raw_data)
        return self._decoded_data

    def __str__(self):
        args = (
            self.task_id, self.tube, self.status, self.space
        )
        return "Task (id: {0}, tube:{1}, status: {2}, space:{3})".format(*args)

    def __del__(self):
        if self.status == 'taken' and not self.modified:
            self.release()

    @classmethod
    def from_tuple(cls, queue, the_tuple):
        if the_tuple is None:
            return
        if the_tuple.rowcount < 1:
            raise Queue.ZeroTupleException('error creating task')
        row = the_tuple[0]
        return cls(
            queue,
            space=queue.space,
            task_id=row[0],
            tube=row[1],
            status=row[2],
            raw_data=row[3],
        )


class Tube(object):
    """
    Tarantol queue tube wrapper. Pinned to space and tube, but unlike Queue
    it has predefined delay, ttl, ttr, and pri.

    .. warning::

        Don't instantiate it with your bare hands
    """
    def __init__(self, queue, name, **kwargs):
        self.queue = queue
        self.opt = {
            'delay' : 0,
            'ttl'   : 0,
            'ttr'   : 0,
            'pri'   : 0,
            'tube'  : name
            }
        self.opt.update(kwargs)
        self._serialize = None
        self._deserialize = None
#----------------
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
                            "or None, but not "+str(type(func)))
        self._serialize = func
#----------------
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
                            "or None, but not "+str(type(func)))
        self._deserialize = func
#----------------
    def update_options(self, **kwargs):
        """
        Update options for current tube (such as ttl, ttr, pri and delay)
        """
        self.opt.update(kwargs)

    def put(self, data, **kwargs):
        """
        Enqueue a task. Returns a tuple, representing the new task.
        The list of fields with task data ('...')is optional.
        If urgent set to True then the task will get the highest priority.

        :param data: Data for pushing into queue
        :param urgent: make task urgent (Not necessary, False by default)
        :param delay: new delay for task (Not necessary, Default of Tube object)
        :param ttl: new time to live (Not necessary, Default of Tube object)
        :param ttr: time to release (Not necessary, Default of Tube object)
        :param tube: name of Tube (Not necessary, Default of Tube object)
        :param pri: priority (Not necessary, Default of Tube object)
        :type ttl: int
        :type delay: int
        :type ttr: int
        :type tube: string
        :type urgent: boolean
        :rtype: `Task` instance
        """
        opt = dict(self.opt, **kwargs)

        method = "queue.put"
        if "urgent" in kwargs and kwargs["urgent"]:
            opt["delay"] = 0
            method = "queue.urgent"

        the_tuple = self.queue.tnt.call(method, (
            str(self.queue.space),
            str(opt["tube"]),
            str(opt["delay"]),
            str(opt["ttl"]),
            str(opt["ttr"]),
            str(opt["pri"]),
            self.serialize(data))
        )

        return Task.from_tuple(self.queue, the_tuple)

    def urgent(self, data=None, **kwargs):
        """
        Same as :meth:`Tube.put() <tarantool_queue.Tube.put>` put, but set highest priority for this task.
        """
        kwargs['urgent'] = True
        return self.put(data, **dict(self.opt, **kwargs))

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

    def kick(self, count=None):
        """
        'Dig up' count tasks in a queue. If count is not given, digs up
        just one buried task.

        :rtype boolean
        """
        return self.queue._kick(self.opt['tube'], count)

    def statistics(self):
        """
        See :meth:`Queue.statistics() <tarantool_queue.Queue.statistics>` for more information.
        """
        return self.queue.statistics(tube=self.opt['tube'])

class Queue(object):
    """
    Tarantool queue wrapper. Surely pinned to space. May create tubes.
    By default it uses msgpack for serialization, but you may redefine
    serialize and deserialize methods.
    You must use Queue only for creating Tubes.
    For more usage, please, look into tests.
    Usage:

        >>> from tntqueue import Queue
        >>> queue = Queue()
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

    @staticmethod
    def basic_serialize(data):
        return msgpack.packb(data)

    @staticmethod
    def basic_deserialize(data):
        return msgpack.unpackb(data)

    def __init__(self, host="localhost", port=33013,  space=0, schema=None):
        if not(host and port):
            raise Queue.BadConfigException("host and port params "
                                           "must be not empty")

        if not isinstance(port, int):
            raise Queue.BadConfigException("port must be int")

        if not isinstance(space, int):
            raise Queue.BadConfigException("space must be int")

        self.host = host
        self.port = port
        self.space = space
        self.schema = schema
        self.tubes = {}
        self._serialize = self.basic_serialize
        self._deserialize = self.basic_deserialize

#----------------
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
                            "or None, but not "+str(type(func)))
        self._serialize = func if not (func is None) else self.basic_serialize
    @serialize.deleter
    def serialize(self):
        self._serialize = self.basic_serialize
#----------------
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
        self._deserialize = func if not (func is None) else self.basic_deserialize
    @deserialize.deleter
    def deserialize(self):
        self._deserialize = self.basic_deserialize
#----------------
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
        if not('call' in dir(cls) and '__init__' in dir(cls)) and not (cls is None):
            raise TypeError("Connection class must have"
                            " connect and call methods or be None")
        self._conclass = cls if not (cls is None) else tarantool.Connection
        if hasattr(self, '_tnt'):
            self.__dict__.pop('_tnt')
    @tarantool_connection.deleter
    def tarantool_connection(self):
        if hasattr(self, '_conclass'):
            self.__dict__.pop('_conclass')
        if hasattr(self, '_tnt'):
            self.__dict__.pop('_tnt')
#----------------
    @property
    def tarantool_lock(self):
        """
        Locking class: must be locking instance with methods __enter__ and __exit__. If
        it sets to None or delete - it will use default threading.Lock() instance
        for locking in the connecting.
        """
        if not hasattr(self, '_lockinst'):
            self._lockinst = threading.Lock()
        return self._lockinst
    @tarantool_lock.setter
    def tarantool_lock(self, lock):
        if not('__enter__' in dir(lock) and '__exit__' in dir(lock)) and not (lock is None):
            raise TypeError("Lock class must have"
                            " `__enter__` and `__exit__` methods or be None")
        self._lockinst = lock if not (lock is None) else threading.Lock()
    @tarantool_lock.deleter
    def tarantool_lock(self):
        if hasattr(self, '_lockinst'):
            self.__dict__.pop('_lockinst')
#----------------

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
        the_tuple = self.tnt.call("queue.take", tuple(args))
        if the_tuple.rowcount == 0:
            return None
        return Task.from_tuple(self, the_tuple)

    def _ack(self, task_id):
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.ack", args)
        return the_tuple.return_code == 0

    def _release(self, task_id, delay=0, ttl=0):
        the_tuple = self.tnt.call("queue.release", (
            str(self.space),
            str(task_id),
            str(delay),
            str(ttl)
        ))
        return Task.from_tuple(self, the_tuple)

    def _requeue(self, task_id):
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.requeue", args)
        return the_tuple.return_code == 0

    def _bury(self, task_id):
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.bury", args)
        return the_tuple.return_code == 0

    def _delete(self, task_id):
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.delete", args)
        return the_tuple.return_code == 0

    def _meta(self, task_id):
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.meta", args)
        if the_tuple.rowcount:
            row = list(the_tuple[0])
            for index in [3, 7, 8, 9, 10, 11, 12]:
                row[index] = unpack_long_long(row[index])
            for index in [6]:
                row[index] = unpack_long(row[index])
            keys = [
                'task_id', 'tube', 'status', 'event', 'ipri',
                'pri', 'cid', 'created', 'ttl', 'ttr', 'cbury',
                'ctaken', 'now'
            ]
            return dict(zip(keys, row))
        return None

    def peek(self, task_id):
        """
        Return a task by task id.

        :param task_id: UUID of task in HEX
        :type task_id: string
        :rtype: `Task` instance
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.peek", args)
        return Task.from_tuple(self, the_tuple)

    def _dig(self, task_id):
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.dig", args)
        return the_tuple.return_code == 0

    def _kick(self, tube, count=None):
        args = [str(self.space), str(tube)]
        if count:
            args.append(str(count))
        the_tuple = self.tnt.call("queue.kick", tuple(args))
        return the_tuple.return_code == 0

    def statistics(self, tube=None):
        """
        Return queue module statistics accumulated since server start.
        Output format: if tube != None, then output is dictionary with
        stats of current tube. If tube is None, then output is dict of
        t stats, ...}
        e.g.:

            >>> tube.statistics()
            # or queue.statistics('tube0')
            # or queue.statistics(tube.opt['tube'])
            {'ack': '233',
            'meta': '35',
            'put': '153',
            'release': '198',
            'take': '431',
            'take_timeout': '320',
            'tasks': {'buried': '0',
                    'delayed': '0',
                    'done': '0',
                    'ready': '0',
                    'taken': '0',
                    'total': '0'},
            'urgent': '80'}
            or
            >>> queue.statistics()
            {'tube0': {'ack': '233',
                    'meta': '35',
                    'put': '153',
                    'release': '198',
                    'take': '431',
                    'take_timeout': '320',
                    'tasks': {'buried': '0',
                            'delayed': '0',
                            'done': '0',
                            'ready': '0',
                            'taken': '0',
                            'total': '0'},
                    'urgent': '80'}}

        :param tube: Name of tube
        :type tube: string or None
        :rtype: dict with statistics
        """
        args = (str(self.space),)
        args = args if tube is None else args + (tube,)
        stat = self.tnt.call("queue.statistics", args)
        ans = {}
        if stat.rowcount > 0:
            for k, v in dict(zip(stat[0][0::2], stat[0][1::2])).iteritems():
                k_t = list(re.match(r'space([^.]*)\.(.*)\.([^.]*)', k).groups())
                if int(k_t[0]) != self.space:
                    continue
                if k_t[1].endswith('.tasks'):
                    k_t = k_t[0:1] + k_t[1].split('.') + k_t[2:3]
                if not (k_t[1] in ans):
                    ans[k_t[1]] = {'tasks': {}}
                if len(k_t) == 4:
                    ans[k_t[1]]['tasks'][k_t[-1]] = v
                elif len(k_t) == 3:
                    ans[k_t[1]][k_t[-1]] = v
                else:
                    raise Queue.ZeroTupleException('stats: \
                            error when parsing respons')
        return ans[tube] if tube else ans

    def _touch(self, task_id):
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.touch", tuple(args))
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
            tube = Tube(self, name, **kwargs)
            self.tubes[name] = tube
        return tube
