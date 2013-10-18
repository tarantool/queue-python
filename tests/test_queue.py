import unittest

from tarantool_queue import Queue
import tarantool

class OpenConnectionTest(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def runTest(self):
        a = Queue("127.0.0.1", 33013, 0)
        conn = a.tnt
        self.assertTrue(isinstance(conn, tarantool.connection.Connection))
        self.assertTrue(isinstance(a.tnt, tarantool.connection.Connection))
        self.assertEqual(a.tnt, conn)

if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
