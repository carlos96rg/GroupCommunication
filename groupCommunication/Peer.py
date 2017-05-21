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
             'print_count', 'receive_ack']

    _ask = Sequencer._ask + ['join_me', 'get_id', 'get_url', 'multicast',
                             'get_identifier', 'get_sequencer']

    _ref = ['multicast', 'join_me', 'start_announcing', 'keep_alive', 'set_sequencer'
            'to_leave', 'define_group', 'set_sequencer', 'get_url', 'get_sequencer',
            'receive_ack']

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
    
    def get_sequencer(self):
        return self.sequencer

    def define_group(self, group):
        self.group = group

    def set_sequencer(self, seq):
        self.sequencer = self.host.lookup_url(seq, 'Peer', 'Peer')

    def join_me(self):
        self.identifier = self.group.join(self.url)
        print "Joined successfully"
        print "My identifier is: ", self.identifier
        self.start_announcing()
        return self.identifier

    def get_id(self):
        return self.id
   
    def get_identifier(self):
        return self.identifier

    def start_announcing(self):
        print "Now I'm in"
        self.interval_reduce = interval(self.host, 5, self.proxy, "keep_alive")

    def to_leave(self):
        print "ready to leave: ", self.url, self.identifier
        self.group.leave(self.url, self.identifier)
        self.interval_reduce.set()

    def keep_alive(self):
        self.group.announce(self.url, self.identifier)

    def multicast(self, message):
        if self.sequencer == self.proxy:
            num = self.get_counter()
        else:
            num = self.sequencer.get_counter()
        for peer_n in self.group.get_members():     # get_members returns url and identifier
            if peer_n[0] is self.url:
                self.receive(message, num)
            else:
                print peer_n[0]
                peer_ref = self.host.lookup_url(peer_n[0], 'Peer', 'Peer')
                peer_ref.receive(message, num, self.id)
        print "Message delivered to everybody"

    def receive(self, message, num, sender):
        print "Message received"
        if self.count < num:    # Get the biggest number as your timestamp
            self.count = num
        self.count += 1
        print "My timestamp now is: ", self.count
        # First, send ack to myself
        ack = list()
        ack.append(self.count)
        self.waiting[message, num] = ack
        if len(self.group.get_members()) is 1:
            self.process_msg(message, sender)
        else:   # Then, send ack to the rest of the group
            for peer_url in self.group.get_members():
                if peer_url[0] == self.url:
                    pass
                else:
                    people = self.host.lookup_url(peer_url[0], 'Peer', 'Peer')
                    people.receive_ack(message, self.count, num, sender)

    def receive_ack(self, message, ack, timestamp, sender):
        if self.count < ack:    # Get the biggest number as your timestamp
            self.count = ack
        print "And now my timestamp is:", self.count
        acks = self.waiting[message, timestamp]
        acks.append(self.count)
        self.waiting[message, timestamp] = acks    # We keep the message and the timestamp
        print "I have this acks: ", len(acks)
        if len(acks) < len(self.group.get_members()):  # If we can process the message
            pass
        else:
            self.process_msg(message, sender)
            del self.waiting[message, timestamp]    # Leave the waiting queue

    def process_msg(self, message, sender):
        if message is not None:     # If it is not None, then message[0] = message message[1] = identifier
            self.messages.append(message)
            print sender, ":", message+"\n:"

if __name__ == "__main__":
    set_context()
    port = raw_input("Insert your port :")
    host = create_host('http://127.0.0.1:'+port)
    print host
    n_peer = raw_input("Write this peer's id:")
    peer = host.spawn(n_peer, Peer)
    e1 = host.lookup_url('http://127.0.0.1:1280/group', 'Group', 'Group')
    peer.define_group(e1)
    print peer.join_me()
    # Message 1
    sleep(2)
    msg = raw_input(": ")
    peer.multicast(msg)
    # Message 2
    msg = raw_input(": ")
    sleep(2)
    peer.multicast(msg)
    # Message 3
    msg = raw_input(": ")
    peer.multicast(msg)
    # Message 4
    msg = raw_input(": ")
    peer.multicast(msg)
    # Message 5
    msg = raw_input(": ")
    peer.multicast(msg)
    print "----------------------------------- leaving the group------------------------"
    sleep(2)
    peer.to_leave()
    sleep(2)
    peer.print_messages()
    shutdown()
