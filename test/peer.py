import unittest
from pyactor.context import set_context, create_host, shutdown, sleep
from groupCommunication.Group import Group
from groupCommunication.Peer import Peer, Sequencer


class TestPeer(unittest.TestCase):

    def setUp(self):
        set_context()
        self.host = create_host('http://127.0.0.1:1280')
        self.host2 = create_host('http://127.0.0.1:1281')
        self.host3 = create_host('http://127.0.0.1:1282')

    def test_sequencer(self):
        sequencer = self.host.spawn('sequencer', Sequencer)
        self.assertEqual(0, sequencer.get_counter())

    def test_peer(self):
        peer = self.host2.spawn('Peer', Peer)
        group = self.host.spawn('group', Group)
        e1 = self.host.lookup_url('http://127.0.0.1:1280/group', 'Group', 'Group')
        # Testing for joining, announcing and leaving the group
        peer.define_group(e1)
        identifier = peer.join_me()
        self.assertEqual(identifier, 0)
        sleep(4)
        self.assertEqual(set(group.get_members()), set([(peer.get_url(), peer.get_identifier())]))
        peer.to_leave()
        # Testing for sending messages
        peer.join_me()
        peer2 = self.host3.spawn('Peer2', Peer)
        peer2.define_group(e1)
        peer2.join_me()
        sleep(3)
        self.assertTrue(peer.get_sequencer() is not None)
        peer.multicast("Hello world!")
        self.assertEqual(peer.print_messages(), peer2.print_messages())

    def tearDown(self):
        shutdown()

    if __name__ == '__main__':
        unittest.main()
