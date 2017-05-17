from pyactor.context import set_context, create_host, interval, serve_forever


class Group(object):
    _tell = ['announce', 'init_start', 'stop_interval', 'reduce_time', 'add_printer', 'print_swarm', 'leave']
    _ask = ['join', 'get_members']
    _ref = ['join', 'announce', 'get_sequencer', 'get_members']

    def __init__(self):
        self.swarm = {}
        self.printer = None
        self.interval_reduce = None
        self.sequencer = None
        self.n_peers = 0
        self.sequencer = None
        self.identifier = 0

    def announce(self, peer_n):
        print peer_n.get_url(), " is announcing"
        print self.n_peers
        self.swarm[peer_n.get_url()] = 20
        if self.n_peers == 1:
            peer_n.set_sequencer(peer_n.get_url())
            self.sequencer = peer_n
            print peer_n.get_id(), "is the sequencer now"
        elif self.n_peers > 1 and self.sequencer is None:
            # If there is not a sequencer, apply bully algorithm
            self.bully()
        else:
            peer_n.set_sequencer(self.sequencer)

    def bully(self):
        print ""

    # the key is the url and the value is the time remaining to cut the connexion
    def join(self, peer):
        print peer
        self.swarm[peer] = 50
        print peer, "has joined"
        self.init_start()
        self.n_peers += 1
        self.identifier += 1
        return self.identifier

    def init_start(self):
        self.interval_reduce = interval(self.host, 1, self.proxy, "reduce_time")

    def stop_interval(self):
        self.interval_reduce.set()

    def leave(self, peer_n):
        try:
            if peer_n is self.sequencer:
                self.sequencer = None
            del self.swarm[peer_n]
            self.n_peers -= 1
            print peer_n, "has left the group"
        except KeyError:
            print "Peer not found"

    def reduce_time(self):
        for peer_ref in self.swarm.keys():
            if self.swarm[peer_ref] < 1:
                self.leave(peer_ref)
            else:
                self.swarm[peer_ref] -= 1

    def get_members(self):
        print self.swarm.keys()
        return self.swarm.keys()

if __name__ == "__main__":
    set_context()
    h = create_host('http://127.0.0.1:1280/')
    e1 = h.spawn('group', Group)
    serve_forever()
