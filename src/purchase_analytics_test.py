import unittest
from purchase_analytics import *


class PurchaseAnalyticsTest(unittest.TestCase):

    def map_func1_unit(self, record):
        """
        Replicate function for map_func1.
        Since we mimic MapReduce in pure Python, making prod_dept_lookup unable to pass in.
        So it is provided within the unit test.
        """
        prod_dept_lookup = {9327: 13, 17461: 12, 17668: 16, 28985: 4, 32665: 3, 33120: 16, 45918: 13, 46667: 4,
                            46842: 3}
        if type(record) != str:
            raise TypeError("Map function 1 expects a string input.")
        record = record.strip().split(",")
        if len(record) != 4:
            raise IndexError("Expecting 4 columns per row in order_products.csv.")
        prod_id, first_ordered = int(record[1]), 1 - int(record[-1])
        return prod_dept_lookup[prod_id], 1, first_ordered

    def test_map_func1(self):
        # typical case
        result = self.map_func1_unit("2,33120,1,1")
        self.assertEqual(result, (16, 1, 0))

        # invalid input type
        with self.assertRaises(TypeError):
            self.map_func1_unit(None)

        # invalid input format
        with self.assertRaises(IndexError):
            self.map_func1_unit("2,33120,1")

    def test_reduce_func(self):
        # typical case
        # since reduce is pipelined from map and shuffle, if there's error, would surface ahead
        result = reduce_func((1, 1), (1, 0))
        self.assertEqual(result, (2, 1))

    def test_map_func2(self):
        # typical case
        # since this map is pipelined from previous steps, if there's error, would surface ahead
        result = map_func2([3, (2, 1)])
        self.assertEqual(result, ('3', '2', '1', '0.50'))

if __name__ == "__main__":
    unittest.main()
