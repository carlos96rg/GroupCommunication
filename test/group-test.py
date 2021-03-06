import unittest
from pyactor.context import set_context, create_host, shutdown, sleep
from groupCommunication.Group import Group
from groupCommunication.Peer import Peer


class GroupTest(unittest.TestCase):

    def setUp(self):
        set_context()
        self.host = create_host('http://127.0.0.1:1280')

    def test_group(self):
        group = self.host.spawn('group', Group)
        identifier = group.join('1')
        group.init_start()
        self.host2 = create_host('http://127.0.0.1:1282')
        peer = self.host2.spawn('peer', Peer)

        self.assertEqual(0, identifier)

        sleep(16)
        self.assertEqual([], group.get_members())

        identifier2 = group.join(peer.get_url())
        self.assertEqual(identifier2, 1)
        group.join('2')
        group.join('3')
        print group.get_members()
        group.leave('2', 2)
        self.assertEqual([('3', 3), ('http://127.0.0.1:1282/peer', 1)], group.get_members())

    def tearDown(self):
        shutdown()

    if __name__ == '__main__':
        unittest.main()
