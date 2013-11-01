import sys
import copy
import msgpack
import unittest
import threading

from tarantool_queue import Queue
import tarantool

class TestSuite_Basic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.queue = Queue("127.0.0.1", 33013, 0)
        cls.tube = cls.queue.tube("tube")

    @classmethod
    def tearDownClass(cls):
        sys.stdout.write("tearDown ...")
        for tube in cls.queue.tubes.values():
            task = tube.take(1)
            while (task is not None):
                task.ack()
                task = tube.take(1)
        print(" ok")

class TestSuite_00_ConnectionTest(TestSuite_Basic):
    def test_00_ConProp(self):
        conn = self.queue.tnt
        self.assertTrue(isinstance(conn, tarantool.connection.Connection))
        self.assertTrue(isinstance(self.queue.tnt, tarantool.connection.Connection))
        self.assertEqual(self.queue.tnt, conn)

    def test_01_Tubes(self):
        tube1 = self.queue.tube("tube1")
        self.assertEqual(self.queue.tube("tube1"), tube1)
        tube1.put([1, 2, 3])
        tube1.put([2, 3, 4])
        self.queue.tube("tube1").take().ack()
        self.queue.tube("tube1").take().ack()

    def test_02_Expire(self):
        with self.assertRaises(AttributeError):
            self.tube.take(1).ack()

    def test_03_TaskMeta(self):
        task = self.tube.put([1, 2, 3, 4])
        task_meta = task.meta()
        self.assertIsInstance(task_meta, dict)
        self.assertEqual(task_meta.keys(), ['status', 'task_id', 'cid', 'ttr', 'tube', 'created', 'pri', 'ctaken', 'ipri', 'cbury', 'ttl', 'now', 'event'])
        self.tube.take().ack()

    def test_04_Stats(self):
        # in our case info for tube less than info for space
        stat_space = self.queue.statistics()
        stat_tube = self.tube.statistics()
        self.assertNotEqual(stat_space, stat_tube)

    def test_05_Urgent(self):
        task1_p = self.tube.put("basic prio")
        task2_p = self.tube.urgent("URGENT TASK")
        task3_p = self.tube.urgent("VERY VERY URGENT TASK")
        task1 = self.tube.take()
        task2 = self.tube.take()
        task3 = self.tube.take()
        self.assertEqual(task1_p.data, task3.data)
        self.assertEqual(task2_p.data, task2.data)
        self.assertEqual(task3_p.data, task1.data)
        task3.release();task1.release();task2.release()
        task1 = self.tube.take()
        task2 = self.tube.take()
        task3 = self.tube.take()
        # They must be in the same order
        self.assertEqual(task1_p.data, task3.data)
        self.assertEqual(task2_p.data, task2.data)
        self.assertEqual(task3_p.data, task1.data)
        task1.ack();task2.ack();task3.ack()

    def test_06_Destructor(self):
        task = self.tube.put("stupid task")
        # analog for del task; gc.gc()
        # task is not taken - must not send exception
        task.__del__()
        task = self.tube.take()
        # task in taken - must not send exception
        task.__del__()
        # task is acked - must not send exception
        self.tube.take().ack()

class TestSuite_01_SerializerTest(TestSuite_Basic):
    def test_00_CustomQueueSerializer(self):
        class A:
            def __init__(self, a = 3, b = 4):
                self.a = a
                self.b = b
            def __eq__(self, other):
                return (isinstance(self, type(other))
                        and self.a == other.a
                        and self.b == other.b)

        self.queue.serialize = (lambda x: msgpack.packb([x.a, x.b]))
        self.queue.deserialize = (lambda x: A(*msgpack.unpackb(x)))
        a = A()
        task1 = self.tube.put(a)
        task2 = self.tube.take()
        self.assertEqual(task1.data, task2.data)
        self.assertEqual(a, task2.data)
        task2.ack()
        self.queue.serialize = self.queue.basic_serialize
        self.queue.deserialize = self.queue.basic_deserialize
        task1 = self.tube.put([1, 2, 3, "hello"])
        task2 = self.tube.take()
        self.assertEqual(task1.data, task2.data)
        task2.ack()

    def test_01_CustomTubeQueueSerializers(self):
        class A:
            def __init__(self, a = 3, b = 4):
                self.a = a
                self.b = b
            def __eq__(self, other):
                return (isinstance(self, type(other))
                        and self.a == other.a
                        and self.b == other.b)

        self.tube.serialize = (lambda x: msgpack.packb([x.a, x.b]))
        self.tube.deserialize = (lambda x: A(*msgpack.unpackb(x)))
        a = A()
        task1 = self.tube.put(a)
        task2 = self.tube.take()
        self.assertEqual(task1.data, task2.data)
        task2.ack()
        self.tube.serialize = None 
        self.tube.deserialize = None
        a = [1, 2, 3, "hello"] 
        task1 = self.tube.put(a)
        task2 = self.tube.take()
        self.assertEqual(task1.data, task2.data)
        self.assertEqual(a, task2.data)
        task2.ack()

    def test_02_CustomMixedSerializers(self):
        class A:
            def __init__(self, a = 3, b = 4):
                self.a = a
                self.b = b
            def __eq__(self, other):
                return (isinstance(self, type(other))
                        and self.a == other.a
                        and self.b == other.b)

        class B:
            def __init__(self, a = 5, b = 6, c = 7):
                self.a = a
                self.b = b
                self.c = c
            def __eq__(self, other):
                return (isinstance(self, type(other))
                        and self.a == other.a
                        and self.b == other.b
                        and self.c == other.c)

        self.queue.serialize = (lambda x: msgpack.packb([x.a, x.b]))
        self.queue.deserialize = (lambda x: A(*msgpack.unpackb(x)))
        self.tube.serialize = (lambda x: msgpack.packb([x.a, x.b, x.c]))
        self.tube.deserialize = (lambda x: B(*msgpack.unpackb(x)))
        b = B()
        task1 = self.tube.put(b)
        task2 = self.tube.take()
        task2.ack()
        self.assertEqual(task1.data, task2.data)
        self.assertEqual(b, task2.data)
        self.tube.serialize = None
        self.tube.deserialize = None
        a = A()
        task1 = self.tube.put(a)
        task2 = self.tube.take()
        task2.ack()
        self.assertEqual(task1.data, task2.data)
        self.assertEqual(a, task2.data)
        self.queue.serialize = self.queue.basic_serialize
        self.queue.deserialize = self.queue.basic_deserialize
        a = [1, 2, 3, "hello"]
        task1 = self.tube.put(a)
        task2 = self.tube.take()
        self.assertEqual(task1.data, task2.data)
        self.assertEqual(a, task2.data)
        task2.ack()

class TestSuite_03_CustomLockAndConnection(TestSuite_Basic):
    def test_00_GoodLock(self):
        class GoodFake(object):
            def __enter__(self):
                pass
            def __exit__(self):
                pass

        self.queue.tarantool_lock = threading.Lock()
        self.queue.tarantool_lock = None
        self.assertTrue(isinstance(self.queue.tarantool_lock, type(threading.Lock())))
        self.queue.tarantool_lock = threading.RLock()
        self.queue.tarantool_lock = GoodFake
        del(self.queue.tarantool_lock)
        self.assertTrue(isinstance(self.queue.tarantool_lock, type(threading.Lock())))
        
    def test_01_GoodConnection(self):
        class GoodFake(object):
            def __init__(self):
                pass
            def call(self):
                pass

        self.queue.tarantool_connection = tarantool.Connection
        self.queue.statistics() # creating basic _tnt
        self.assertTrue(hasattr(self.queue, '_tnt')) # check that it exists
        self.queue.tarantool_connection = None # delete _tnt
        self.assertFalse(hasattr(self.queue, '_tnt')) # check that it doesn't exists
        self.assertEqual(self.queue.tarantool_connection, tarantool.Connection)
        self.queue.tarantool_connection = GoodFake
        del(self.queue.tarantool_connection)
        self.assertEqual(self.queue.tarantool_connection, tarantool.Connection)

    def test_02_BadLock(self):
        class BadFake(object):
            pass
        with self.assertRaises(TypeError):
            self.queue.tarantool_lock = BadFake
        with self.assertRaises(TypeError):
            self.queue.tarantool_lock = (lambda x: x)

    def test_03_BadConnection(self):
        class BadFake(object):
            pass
        with self.assertRaises(TypeError):
            self.queue.tarantool_lock = BadFake
        with self.assertRaises(TypeError):
            self.queue.tarantool_lock = (lambda x: x)

    def test_04_OverallTest(self):
        self.queue.tarantool_lock = threading.Lock()
        self.queue.tarantool_connection = tarantool.Connection
        self.assertIsNotNone(self.queue.statistics())
