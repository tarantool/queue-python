import copy
import msgpack
import unittest

from tntqueue import Queue
import tarantool

class OpenConnectionTest(unittest.TestCase):
    def setUp(self):
        self.queue = Queue("127.0.0.1", 33013, 0)
        self.tube = self.queue.create_tube("tube")

    def tearDown(self):
        task = self.tube.take(1)
        while (task is not None):
            task.ack()
            task = self.tube.take(1)

    def test_00_ConProp(self):
        conn = self.queue.tnt
        self.assertTrue(isinstance(conn, tarantool.connection.Connection))
        self.assertTrue(isinstance(self.queue.tnt, tarantool.connection.Connection))
        self.assertEqual(self.queue.tnt, conn)

    def test_01_Tubes(self):
        tube1 = self.queue.create_tube("tube1")
        self.assertEqual(self.queue.create_tube("tube1"), tube1)
        tube1.put([1, 2, 3])
        tube1.put([2, 3, 4])
        self.queue.create_tube("tube1").take().ack()
        self.queue.create_tube("tube1").take().ack()

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
        # in our case overall==space, but less than tube stat.
        stat_overall = self.queue.statistics(True)
        stat_space = self.queue.statistics()
        stat_tube = self.tube.statistics()
        self.assertEqual(stat_overall, stat_space)
        self.assertNotEqual(stat_overall, stat_tube)

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
        task.__del__() # task is not taken - must not send exceptipn
        task = self.tube.take()
        task.__del__() # task in taken - must not send exception
        self.tube.take().ack() # task is acked - must not send exception
    
    def test_08_CustomSerializer(self):
        #This test must be the last test in this group
        class A:
            def __init__(self, a = 3, b = 4):
                self.a = a
                self.b = b
        self.queue.serialize = (lambda x: msgpack.packb([x.a, x.b]))
        self.queue.deserialize = (lambda x: A(*msgpack.unpackb(x)))
        a = A(5, 6)
        task1 = self.tube.put(a)
        task2 = self.tube.take()
        self.assertEqual(task1.data.a, task2.data.a)
        self.assertEqual(task1.data.b, task2.data.b)

if __name__ == "__main__":
    unittest.main(verbosity=2)
