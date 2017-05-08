from pyactor.context import set_context, create_host, interval, serve_forever


class Group(object):
    _tell = ['announce', 'init_start', 'stop_interval', 'reduce_time', 'add_printer', 'print_swarm']
    _ask = ['join', 'get_members']
    _ref = ['join', 'announce', 'get_peers', 'get_sequencer']

    def __init__(self):
        self.swarm = {}
        self.printer = None
        self.interval_reduce = None
        self.sequencer = None

    def announce(self, peer_n):
            print peer_n, " is announcing"
            self.swarm[peer_n] = 10

    # the key is the id ('peer1') and the value is the time remain to cut the connexion
    def join(self, peer):
        print peer
        self.swarm[peer] = 10
        print peer, "has joined"
        self.init_start(peer)
        return True

    def init_start(self, peer):
        self.interval_reduce = interval(self.host, 1, self.proxy, "reduce_time", peer)

    def stop_interval(self):
        self.interval_reduce.set()

    def leave(self, peer_n):
        try:
            del self.swarm[peer_n]
            print "Peer deleted successfully"
        except KeyError:
            print "Peer not found"

    def reduce_time(self, peer):
        if self.swarm[peer] <= 0:
            self.leave(peer)
            self.stop_interval()
        else:
            self.swarm[peer] -= 1

    def get_members(self, torrent_id):
        peer_list = self.swarm[torrent_id].keys()
        return peer_list

    def print_swarm(self):
        print "Hello world!"

    def get_sequencer(self):
        print ""
        # Pasar el lider a quien lo pida

    def set_sequenecer(self):
        print ""
        # Algoritmo del bully


class Sequencer(object):

    def get_counter(self):
        print ""


class Peer(Sequencer):
    _tell = ['to_leave', 'keep_alive']
    _ask = []
    _ref = ['join_me', 'to_leave', 'keep_alive']

    def __init__(self):
        self.group = None
        self.interval_reduce = None
        self.leader = None

    def join_me(self):
        self.group.join(self.proxy)

    def process_message(self):
        print ""

    def start_announcing(self):
        self.interval_reduce = interval(self.host, 3, self.proxy, "keep_alive")

    def to_leave(self):
        self.interval_reduce.set()
        self.group.leave(self)

    def keep_alive(self):
        self.group.announce(self)

if __name__ == "__main__":
    set_context()
    h = create_host('http://127.0.0.1:1280/')
    e1 = h.spawn('group', Group)
    serve_forever()



