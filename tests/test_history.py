import unittest
from scripts.check_history import check


class CheckerTest(unittest.TestCase):
    def test_finds_stale_read(self):
        events = [{"start": 0, "end": 1, "op": "put", "value": "new", "result": None},
                  {"start": 2, "end": 3, "op": "get", "value": None, "result": "old"}]
        self.assertFalse(check(events))

    def test_overlapping_write_and_read(self):
        events = [{"start": 0, "end": 3, "op": "put", "value": "new", "result": None},
                  {"start": 1, "end": 2, "op": "get", "value": None, "result": None}]
        self.assertTrue(check(events))


if __name__ == "__main__":
    unittest.main()
