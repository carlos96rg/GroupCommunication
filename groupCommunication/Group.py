from pyactor.context import set_context, create_host, interval, serve_forever


class Group(object):
    _tell = ['announce', 'init_start', 'stop_interval', 'reduce_time',
             'add_printer', 'print_swarm', 'leave']
    _ask = ['join', 'get_members', 'get_sequencer']
    _ref = ['join', 'announce', 'get_sequencer', 'get_members']

    def __init__(self):
        self.swarm = {}
        self.printer = None
        self.interval_reduce = None
        self.sequencer = None
        self.n_peers = 0
        self.sequencer = None
        self.identifier = 0

    def announce(self, peer_n, identifier):
        print peer_n, " is announcing"
        print self.n_peers
        self.swarm[peer_n, identifier] = 20
        peer_ref = self.host.lookup_url(peer_n, 'Peer', 'Peer')
        if self.n_peers == 1:
            peer_ref.set_sequencer(peer_n)
            self.sequencer = peer_n
            print peer_ref.get_id(), "is the sequencer now"
        else:
            peer_ref.set_sequencer(self.sequencer)

    def get_sequencer(self):
        return self.sequencer

    # the key is the url and the value is the time remaining
    # to cut the connexion
    def join(self, peer):
        print peer
        self.swarm[peer, self.identifier] = 20
        print peer, "has joined"
        self.init_start()
        self.n_peers += 1
        self.identifier += 1
        return self.identifier-1

    def init_start(self):
        self.interval_reduce = interval(self.host, 1, self.proxy,
                                        "reduce_time")

    def stop_interval(self):
        self.interval_reduce.set()

    def leave(self, peer_n, identifier):
        try:
            print peer_n, "with identifier", identifier, "is leaving"
            if peer_n is self.sequencer:
                self.sequencer = None
            del self.swarm[peer_n, identifier]
            self.n_peers -= 1
            print peer_n, "has left the group"
        except KeyError:
            print "Peer not found"

    def reduce_time(self):
        print self.swarm
        for peer_ref in self.swarm.keys():
            if self.swarm[peer_ref] < 1:
                self.leave(peer_ref[0], peer_ref[1])
            else:
                self.swarm[peer_ref] -= 1

    def get_members(self):
        print self.swarm.keys()
        return self.swarm.keys()


if __name__ == "__main__":
    set_context()
    host = create_host('http://127.0.0.1:1280/')
    e1 = host.spawn('group', Group)
    serve_forever()
