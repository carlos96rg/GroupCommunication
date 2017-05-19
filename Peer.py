from pyactor.context import set_context, create_host, sleep, shutdown, interval


class Sequencer(object):
    _ask = ['get_counter']

    def __init__(self):
        self.count = 0

    def get_counter(self):
        print self.count
        self.count += 1
        return self.count - 1


class Peer(Sequencer):

    _tell = ['print_messages', 'set_sequencer', 'start_announcing', 'to_leave',
             'define_group', 'process_msg', 'keep_alive', 'receive',
             'print_count']

    _ask = Sequencer._ask + ['join_me', 'get_id', 'get_url', 'multicast']

    _ref = ['multicast', 'join_me', 'start_announcing', 'keep_alive', 'set_sequencer'
            'to_leave', 'define_group', 'set_sequencer', 'get_url']

    def __init__(self):
        super(Peer, self).__init__()
        self.group = None
        self.interval_reduce = None
        self.sequencer = None
        self.count = 0
        self.messages = []
        self.waiting = {}   # Where we will keep the messages that are not ready to be received
        self.identifier = None

    def print_messages(self):
        print self.messages

    def get_url(self):
        return self.url

    def define_group(self, group):
        self.group = group

    def set_sequencer(self, seq):
        self.sequencer = seq

    def join_me(self):
        self.identifier = self.group.join(self.url)
        print "Joined successfully"
        print "My identifier is: ", self.identifier
        self.start_announcing()

    def get_id(self):
        return self.id

    def start_announcing(self):
        print "Now I'm in"
        self.interval_reduce = interval(self.host, 5, self.proxy, "keep_alive")

    def to_leave(self):
        self.interval_reduce.set()
        self.group.leave(self.url)

    def keep_alive(self):
        self.group.announce(self.proxy, self.identifier)

    def multicast(self, message):
        if self.sequencer == self.proxy:
            num = self.get_counter()
        else:
            num = self.sequencer.get_counter()
        for peer_n in self.group.get_members():     # get_members returns url and identifier

            if peer_n[0] is self.url:
                self.receive(message, num, self.id)
            else:
                print peer_n[0]
                peer_ref = self.host.lookup_url(peer_n[0], 'Peer', 'Peer')
                peer_ref.receive(message, num, self.id)
        print "Message delivered to everybody"

    def receive(self, msg, num, identifier):
        print "Message received"
        if len(self.messages) == num:
            self.process_msg(msg, identifier)
        else:
            self.waiting[num] = msg
            try:    # Look if the next message has been sent
                self.process_msg(self.waiting[len(self.messages)-1], identifier)
            except KeyError:
                pass

    def process_msg(self, message, identifier):
        self.messages.append(message)
        print identifier, ":", message

if __name__ == "__main__":
    set_context()
    port = raw_input("Insert your port :")
    host = create_host('http://127.0.0.1:'+port)
    print host
    n_peer = raw_input("Write this peer's id:")
    peer = host.spawn(n_peer, Peer)
    e1 = host.lookup_url('http://127.0.0.1:1280/group', 'Group', 'Group')
    peer.define_group(e1)
    peer.join_me()
    # Message 1
    sleep(2)
    msg = raw_input(n_peer+": ")
    peer.multicast(msg)
    # Message 2
    msg = raw_input(n_peer+": ")
    sleep(2)
    peer.multicast(msg)
    # Message 3
    msg = raw_input(n_peer+": ")
    peer.multicast(msg)
    # Message 4
    msg = raw_input(n_peer+": ")
    peer.multicast(msg)
    # Message 5
    msg = raw_input(n_peer+": ")
    peer.multicast(msg)

    peer.to_leave()
    peer.print_messages()
    shutdown()
