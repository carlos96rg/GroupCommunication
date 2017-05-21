import unittest
from pyactor.context import set_context, create_host, shutdown, sleep
from groupCommunication import Group


class GroupTest(unittest.TestCase):

    def setUp(self):
        set_context()
        self.host = create_host()

    def test_group(self):
        group = self.host.spawn('group', Group)
        identifier = group.join('1')

        self.assertEqual(0, identifier)

        sleep(10)

        self.assertEqual(set([]), set(group.get_members()))

        group.join('2')
        group.join('3')
        group.leave('2', 1)

        self.assertEqual(set([('3', 2)]), set(group.get_members()))

    def tearDown(self):
        shutdown()

    if __name__ == '__main__':
        unittest.main()
