import unittest
import sys
from esgi.exo.FootballApp.FootballApp import FootballApp

class FootballAppTest(unittest.TestCase):
	def __init__(self, argv):
		self.footballApp = FootballApp(sys.argv)

	def test_one_plus_one_is_two(self):
		self.assertEqual(2, 1 + 1)

if __name__ == '__main__':
	unittest.main()