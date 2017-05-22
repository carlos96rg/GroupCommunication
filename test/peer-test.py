import unittest
from pyactor.context import set_context, create_host, shutdown, sleep
from groupCommunication.Group import Group
from groupCommunication.Peer import Peer, Sequencer, Lamport


class TestPeer(unittest.TestCase):

    def setUp(self):
        self.host1 = create_host('http://127.0.0.1:1285')
        self.host2 = create_host('http://127.0.0.1:1281')
        self.host3 = create_host('http://127.0.0.1:1282')

    def test_peer(self):
        group = self.host1.spawn('group', Group)
        peer = self.host2.spawn('peer', Peer)
        e1 = self.host1.lookup_url('http://127.0.0.1:1285/group', 'Group', 'Group')
        peer.define_group(e1)
        peer.join_me()
        self.assertEqual(group.get_members(), [('http://127.0.0.1:1281/peer', 0)])

    def test_sequencer(self):
        sequencer = self.host1.spawn('sequencer', Sequencer)
        self.assertEqual(0, sequencer.get_counter())
        peer = self.host2.spawn('Peer', Sequencer)
        group = self.host1.spawn('group', Group)
        e1 = self.host1.lookup_url('http://127.0.0.1:1285/group', 'Group', 'Group')
        # Testing for joining, announcing and leaving the group
        peer.define_group(e1)
        sequencer.define_group(e1)
        identifier = sequencer.join_me()
        self.assertEqual(identifier, 0)
        sleep(4)
        self.assertEqual(group.get_members(), [('http://127.0.0.1:1285/sequencer', 0)])
        sequencer.to_leave()
        # Testing for sending messages
        sequencer.join_me()
        identifier = peer.join_me()
        self.assertEqual(identifier, 2)
        peer2 = self.host3.spawn('Peer2', Sequencer)
        peer2.define_group(e1)
        peer2.join_me()
        self.assertTrue(group.get_sequencer() is not None)

    def test_lamport(self):
        peer = self.host2.spawn('Peer', Lamport)
        group = self.host1.spawn('group', Group)
        e1 = self.host1.lookup_url('http://127.0.0.1:1285/group', 'Group', 'Group')
        # Testing for joining, announcing and leaving the group
        peer.define_group(e1)
        identifier = peer.join_me()
        self.assertEqual(identifier, 0)
        sleep(4)
        self.assertEqual(set(group.get_members()), set([(peer.get_url(), peer.get_identifier())]))
        peer.to_leave()
        # Testing for sending messages
        identifier = peer.join_me()
        self.assertEqual(identifier, 1)
        peer2 = self.host3.spawn('Peer2', Lamport)
        peer2.define_group(e1)
        peer2.join_me()
        sleep(3)

    def tearDown(self):
        shutdown()

    if __name__ == '__main__':
        unittest.main()
