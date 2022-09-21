import unittest

from packageName.module import Class


class TestModule(unittest.TestCase):
    def test_function(self):
        c = Class()
        self.assertIsNone(c.function(0))
        self.assertRaises(TypeError, c.function)


if __name__ == "__main__":
    unittest.main()
