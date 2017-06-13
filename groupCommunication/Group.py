from pyactor.context import set_context, create_host, interval, serve_forever


class Group(object):
    _tell = ['announce', 'init_start', 'stop_interval', 'reduce_time',
             'add_printer', 'print_swarm', 'leave', 'set_sequencer',
             'election_started', 'election_finished', 'set_count',
             'set_n_messages', 'kill_actor']
    _ask = [
        'join',
        'get_members',
        'get_sequencer',
        'get_election_in_process',
        'get_count',
        'get_n_messages']
    _ref = ['join', 'announce', 'get_sequencer', 'get_members']

    def __init__(self):
        self.swarm = {}
        self.printer = None
        self.interval_reduce = None
        self.sequencer = None
        self.n_peers = 0
        self.identifier = 0
        self.election_in_process = False
        # Used in bully
        self.count = 0
        self.n_messages = 0

    def announce(self, peer_n, identifier):
        print peer_n, " is announcing"
        print self.n_peers
        if (peer_n, identifier) not in self.swarm:
            # If an inactive member returns without a join it gets removed
            peer_ref = self.host.lookup_url(peer_n, 'Peer', 'Peer')
            peer_ref.to_leave()
            peer_ref.kill_actor()
        else:
            self.swarm[peer_n, identifier] = 15

    def get_sequencer(self):
        return self.sequencer

    def set_sequencer(self, seq):
        self.sequencer = seq
        print "## Group: the new sequencer is ", seq

    # election_in_process avoids the removal of members while an election is
    # happening
    def election_started(self):
        self.election_in_process = True

    def election_finished(self):
        self.election_in_process = False

    def get_election_in_process(self):
        return self.election_in_process

    # the key is the url and the value is the time remaining
    # to cut the connexion
    def join(self, peer):
        print peer
        self.swarm[peer, self.identifier] = 15
        print peer, "has joined"
        self.n_peers += 1
        self.identifier += 1
        peer_ref = self.host.lookup_url(peer, 'Peer', 'Peer')
        if self.n_peers == 1:
            peer_ref.set_sequencer(peer)
            self.sequencer = peer
            print peer, "is the sequencer now"
        else:
            print self.sequencer, "is the sequencer"
            peer_ref.set_sequencer(self.sequencer)
        return self.identifier - 1

    def init_start(self):
        self.interval_reduce = interval(self.host, 1, self.proxy,
                                        "reduce_time")

    def stop_interval(self):
        self.interval_reduce.set()

    def leave(self, peer_n, identifier):
        try:
            print peer_n, "with identifier", identifier, "is leaving"
            if peer_n == self.sequencer:
                self.sequencer = None
            del self.swarm[peer_n, identifier]
            self.n_peers -= 1
            print peer_n, "has left the group"
        except KeyError:
            print "Peer not found"

    def reduce_time(self):
        if self.election_in_process is False:
            print self.swarm
            for peer_ref in self.swarm.keys():
                if self.swarm[peer_ref] < 1:
                    self.leave(peer_ref[0], peer_ref[1])
                else:
                    self.swarm[peer_ref] -= 1
        else:
            print "Election in process..."

    def get_members(self):
        print self.swarm.keys()
        return self.swarm.keys()

    def set_count(self, count):
        self.count = count

    def get_count(self):
        return self.count

    def set_n_messages(self, number_of_messages):
        self.n_messages = number_of_messages

    def get_n_messages(self):
        return self.n_messages


if __name__ == "__main__":
    set_context()
    host = create_host('http://127.0.0.1:1280/')
    e1 = host.spawn('group', Group)
    e1.init_start()
    serve_forever()
