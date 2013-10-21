# -*- coding: utf-8 -*-

# client for tarantool's queue
# https://github.com/tarantool/queue
# usage:
# queue = Queue()
# tube = queue.create_tube("tasks1", ttl=60, delay=5)
# tube.put()
# task = tube.put()
# task = tube.put()
# task = tube.take()
# task = tube.take()
# task = tube.take()

import tarantool
from tarantool.error import DatabaseError, NetworkError
from threading import Lock
import struct

import msgpack


def unpack_long_long(value):
    return struct.unpack("<q", value)[0]

def unpack_long(value):
    return struct.unpack("<l", value)[0]


class Task(object):
    """
    Tarantool queue task wrapper. 
    """

    def __init__(self, queue, space=0, task_id=0, tube="", status="", raw_data=None):
        self.task_id = task_id
        self.tube = tube
        self.status = status
        self.raw_data = raw_data
        self.space = space
        self.queue = queue
        self.modified = False

    def ack(self):
        self.modified = True
        return self.queue.ack(self.task_id)

    def release(self, **kwargs):
        self.modified = True
        return self.queue.release(self.task_id, **kwargs)

    def delete(self):
        self.modified = True
        return self.queue.delete(self.task_id)

    def requeue(self):
        self.modified = True
        return self.queue.requeue(self.task_id)

    def done(self):
        self.modified = True
        return self.queue.done(self.task_id)

    def bury(self):
        self.modified = True
        return self.queue.bury(self.task_id)

    def dig(self):
        self.modified = True
        return self.queue.dig(self.task_id)

    def meta(self):
        return self.queue.meta(self.task_id)

    def touch(self):
        return self.queue.touch(self.task_id)

    def _data(self):
        if not self.raw_data:
            return None
        if not hasattr(self, '_decoded_data'):
            self._decoded_data = self.queue.deserialize(self.raw_data)
        return self._decoded_data

    def __str__(self):
        args = (
            self.task_id, self.tube, self.status, self.space
        )
        return "Task (id: {0}, tube:{1}, status: {2}, space:{3})".format(*args)

    def __del__(self):
        if self.status == 'taken' and not self.modified:
            self.release()

    data = property(_data)

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

    def update_options(self, **kwargs):
        self.opt.update(kwargs)

    def put(self, data=None, urgent=False, **kwargs):
        return self.queue.put(data, True, **dict(self.opt, **kwargs))
    
    def urgent(self, data=None, **kwargs):
        return self.queue.put(data, True, **dict(self.opt, **kwargs))

    def take(self, timeout=0):
        return self.queue.take(self.opt['tube'], timeout)
    
    def kick(self, count=None):
        return self.queue.kick(self.opt['tube'], count)

    def statisticts(self):
        return self.queue.statistics(tube=self.opt['tube'])

class Queue(object):
    """
    Tarantool queue wrapper. Surely pinned to space. May create tubes.
    By default it uses msgpack for serialization, but you may redefine
    serialize and deserialize methods.
    You must use Queue only for creating Tubes.
    Usage:
        >>> from tntqueue import Queue
        >>> queue = tntqueue.Queue()
        >>> tube1 = queue.create_tube('holy_grail', ttl=100, delay=5)
        >>> tube1.put([1, 2, 3])    # Put task into the queue
        >>> tube1.urgent([2, 3, 4]) # Put task into the beggining of queue (Highest priority).
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
        >>> tube1.take().ack() # take task, make what it needs and 
            True
    """

    DataBaseError = DatabaseError
    NetworkError = NetworkError

    class BadConfigException(Exception):
        pass

    class ZeroTupleException(Exception):
        pass

    def serialize(self, data):
        return msgpack.packb(data)

    def deserialize(self, data):
        return msgpack.unpackb(data)

    def __init__(self, host="localhost", port=33013,  space=0, schema=None):
        if not(host and port):
            raise Queue.BadConfigException("host and port params must be not empty")

        if not isinstance(port, int):
            raise Queue.BadConfigException("port must be int")

        if not isinstance(space, int):
            raise Queue.BadConfigException("space must be int")

        self.host = host
        self.port = port
        self.space = space
        self.schema = schema
        self._instance_lock = Lock()
        self.tubes = {}

    def tnt_instance(self):
        with self._instance_lock:
            if not hasattr(self, '_tnt'):
                self._tnt = tarantool.connect(self.host, self.port, schema=self.schema)
        return self._tnt

    tnt = property(tnt_instance)

    def put(self, data, urgent=False, tube=None, **kwargs):
        """
        Enqueue a task. Returns a tuple, representing the new task.
        The list of fields with task data ('...')is optional.
        If urgent set to True then the task will get the highest priority.
        """
        opt = {
            'tube': tube,
            'pri': 0,  # priority
            'delay': 0,  # delay for task
            'ttl': 0,  # time to live
            'ttr': 0  # time to release
        }
        opt.update(kwargs)

        method = "queue.put"
        if urgent:
            opt['delay'] = 0
            method = "queue.urgent"

        the_tuple = self.tnt.call(method, (
            str(self.space),
            str(opt["tube"]),
            str(opt["delay"]),
            str(opt["ttl"]),
            str(opt["ttr"]),
            str(opt["pri"]),
            self.serialize(data))
        )
        return Task.from_tuple(self, the_tuple)

    def take(self, tube, timeout=0):
        """
        If there are tasks in the queue ready for execution,
        take the highest-priority task. Otherwise, wait for a
        ready task to appear in the queue, and, as soon as it appears,
        mark it as taken and return to the consumer. If there is a
        timeout, and the task doesn't appear until the timeout expires,
        return 'nil'. If timeout is None, wait indefinitely until
        a task appears.
        """
        args = [str(self.space), str(tube)]
        if timeout is not None:
            args.append(str(timeout))
        the_tuple = self.tnt.call("queue.take", tuple(args))
        if the_tuple.rowcount == 0:
            return None
        return Task.from_tuple(self, the_tuple)

    def ack(self, task_id):
        """
        Confirm completion of a task. Before marking a task as complete
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.ack", args)
        return the_tuple.return_code == 0

    def release(self, task_id, delay=0, ttl=0):
        """
        Return a task back to the queue: the task is not executed.
        """
        the_tuple = self.tnt.call("queue.release", (
            str(self.space),
            str(task_id),
            str(delay),
            str(ttl)
        ))
        return Task.from_tuple(self, the_tuple)

    def requeue(self, task_id):
        """
        Return a task to the queue, the task is not executed.
        Puts the task at the end of the queue, so that it's
        executed only after all existing tasks in the queue are
        executed.
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.requeue", args)
        return the_tuple.return_code == 0

    def bury(self, task_id):
        """
        Mark a task as buried. This special status excludes the
        task from the active list, until it's dug up. This function
        is useful when several attempts to execute a task lead to a
        failure. Buried tasks can be monitored by the queue owner,
        and treated specially.
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.bury", args)
        return the_tuple.return_code == 0

    def done(self, task_id, data=None):
        """
        Mark a task as complete (done), but don't delete it.
        Replaces task data with the supplied data.
        """
        the_tuple = self.tnt.call("queue.done", (
            str(self.space),
            str(task_id),
            self.serialize(data))
        )
        return the_tuple.return_code == 0

    def delete(self, task_id):
        """
        Delete a task from the queue (regardless of task state or status).
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.delete", args)
        return the_tuple.return_code == 0

    def meta(self, task_id):
        """
        Return unpacked task metadata.
        """
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
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.peek", args)
        return Task.from_tuple(self, the_tuple)

    def dig(self, task_id):
        """
        'Dig up' a buried task, after checking that the task is buried.
        The task status is changed to ready.
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.dig", args)
        return the_tuple.return_code == 0

    def kick(self, tube, count=None):
        """
        'Dig up' count tasks in a queue. If count is not given, digs up
        just one buried task.
        """
        args = [str(self.space), str(tube)]
        if count:
            args.append(str(count))
        the_tuple = self.tnt.call("queue.kick", tuple(args))
        return the_tuple.return_code == 0

    def statistics(self, overall = False, tube = None):
        """
        Return queue module statistics accumulated since server start.
        """
        args = tuple() if overall else (str(self.space),)
        args = args if overall or not tube else args + (tube,)
        stat = self.tnt.call("queue.statistics", args)
        if stat.rowcount > 0:
            return dict(zip(stat[0][0::2], stat[0][1::2]))
        return dict()
    
    def touch(self, task_id):
        """
        Prolong living time for taken task with this id.
        """
        args = (str(self.space), task_id)
        the_tuple = self.tnt.call("queue.touch", tuble(args))
        return the_tuple.return_code == 0

    def create_tube(self, name, **kwargs):
        """
        Create Tube object, if not created before, and set kwargs.
        If existed, return existed Tube.
        """ 
        if name in self.tubes:
            tube = self.tubes[name]
            tube.update_options(**kwargs)
        else:
            tube = Tube(self, name, **kwargs)
            self.tubes[name] = tube
        return tube
