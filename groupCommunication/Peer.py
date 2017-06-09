from pyactor.context import set_context, create_host, sleep, shutdown, interval
from pyactor.exceptions import TimeoutError

class Peer(object):

    _tell = ['print_messages', 'set_sequencer', 'start_announcing', 'to_leave',
             'define_group', 'process_msg', 'keep_alive', 'receive',
             'print_count', 'receive_ack']

    _ask = ['join_me', 'get_id', 'get_url', 'get_identifier', 'get_sequencer']

    _ref = ['join_me', 'start_announcing', 'keep_alive', 'set_sequencer',
            'to_leave', 'define_group', 'get_url', 'get_sequencer']

    def __init__(self):
        self.group = None
        self.interval_reduce = None
        self.sequencer = None
        self.count = 0
        self.messages = []
        self.waiting = {}
        # Where we will keep the messages that are not ready to be received
        self.identifier = None

    def print_messages(self):
        print self.messages

    def get_url(self):
        return self.url

    def define_group(self, group):
        self.group = group

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
        self.interval_reduce = interval(self.host, 5, self.proxy, "keep_alive")

    def to_leave(self):
        print "ready to leave: ", self.url, self.identifier
        self.group.leave(self.url, self.identifier)
        self.interval_reduce.set()

    def keep_alive(self):
        self.group.announce(self.url, self.identifier)


class Sequencer(Peer):

    _tell = Peer._tell + ['process_msg', 'receive', 'multicast', 'multicast_bully',             'multicast_delay']
    _ask = Peer._ask + ['get_counter', 'initiate_election']
    _ref = Peer._ref + ['multicast', 'initiate_election', 'multicast_bully','multicast_delay']

    def __init__(self):
        super(Sequencer, self).__init__()
        self.count = 0

    def get_sequencer(self):
        return self.sequencer

    def set_sequencer(self, seq):
        self.sequencer = self.host.lookup_url(seq, 'Sequencer', 'Peer')

    def get_counter(self):
        self.count += 1
        return self.count - 1

    def multicast(self, message):
        if self.sequencer == self.proxy:
            num = self.get_counter()
        else:
            num = self.sequencer.get_counter()
        #if self.identifier == 1:
        #    sleep(5)
        for peer_n in self.group.get_members():
            if peer_n[0] is self.url:
                self.receive(message, num)
            else:
                peer_ref = self.host.lookup_url(peer_n[0], 'Sequencer', 'Peer')
                peer_ref.receive(message, num, self.id)
        print "Message delivered to everybody"

    def multicast_delay(self, message):
        print self.sequencer
        if self.sequencer == self.proxy:
            num = self.get_counter()
            sleep(5)
        else:
            num = self.sequencer.get_counter()
            sleep(5)
        for peer_n in self.group.get_members():
            if peer_n[0] is self.url:
                self.receive(message, num)
            else:
                print peer_n[0]
                peer_ref = self.host.lookup_url(peer_n[0], 'Sequencer', 'Peer')
                peer_ref.receive(message, num, self.id)
        print "Message delivered to everybody"

    def receive(self, message, num, sender):
        print "Message received"
        if len(self.messages) == num:
            self.process_msg(message, sender)
            try:
                for key in self.waiting.keys():
                    values = self.waiting[key]
                    key_message = values[0]
                    sndr = values[1]
                    self.process_msg(key_message, sndr)
                    del self.waiting[key]
            except KeyError:
                pass
        else:
            self.waiting[num] = list([message, sender])
        print "WAITING ",self.waiting
        print "MESSAGES ",self.messages

    def process_msg(self, message, sender):
            self.messages.append(message)
            print sender, ":", message+"\n:"

    # If the sequencer has fallen a new one is chosen
    def initiate_election(self):
        print "Initiate election"
        members = self.group.get_members()
        print "MEMBERS: ", members
        winner = ("TEST", -1)
        for member in members:
            if member[1] > winner[1]:  # Get peer with larger indentifier
                winner = member

        for member in members:
            if member[0] == self.url:
                self.sequencer = self.host.lookup_url(winner[0], 'Sequencer', 'Peer')
            else:
                 peer_ref = self.host.lookup_url(member[0], 'Sequencer', 'Peer')
                 peer_ref.set_sequencer(winner[0])
           
        self.group.set_sequencer(winner[0])
        #election_in_process is False
        self.group.election_finished()

    # Supports sequencer failure using bully election algorithm
    def multicast_bully(self, message):
        if self.sequencer == self.proxy:
            num = self.get_counter()
        else:
            try:
                num = self.sequencer.get_counter(timeout=3)
            except TimeoutError as e:
                print "The sequencer has fallen"
                #sleep(self.identifier)  # Sleep inversely proportional to identifier
                print "Election in process", election_in_process
                if self.group.get_election_in_process() is False:
                    #election_in_process is True
                    self.group.election_started()
                    print "HEYY",self.group.get_election_in_process()
                    self.initiate_election()

                    #self.multicast_bully(message)
                    if self.sequencer == self.proxy:
                        print "SEQ==PROXY"
                        num = self.get_counter()
                    else:
                        print "ELSE: ",self.sequencer
                        num = self.sequencer.get_counter()
                # Hay que enviar una senal para parar a los demas peers
                # mientras se elige el sequencer

        for peer_n in self.group.get_members():  # get_members returns url and identifier
            if peer_n[0] is self.url:
                self.receive(message, num)
            else:
                print peer_n[0]
                peer_ref = self.host.lookup_url(peer_n[0], 'Sequencer', 'Peer')
                peer_ref.receive(message, num, self.id)
        print "Message delivered to everybody"


class Lamport(Peer):
    _ask = Peer._ask + []
    _tell = Peer._tell + ['set_sequencer', 'to_leave', 'process_msg',
                          'receive', 'receive_ack', 'multicast',
                          'deliver_queue']
    _ref = Peer._ref + ['multicast', 'set_sequencer', 'set_sequencer',
                        'receive_ack']

    def __init__(self):
        super(Lamport, self).__init__()
        self.count = 0
        self.identifier = None
        self.queue = []
        # Where we will keep the messages that are not ready to be received

    def set_sequencer(self, seq):
        self.sequencer = self.host.lookup_url(seq, 'Lamport', 'Peer')

    def multicast(self, message):
        num = self.count
        for peer_n in self.group.get_members():
            # get_members returns url and identifier
            if peer_n[0] is self.url:
                self.receive(message, num)
            else:
                print peer_n[0]
                peer_ref = self.host.lookup_url(peer_n[0], 'Lamport', 'Peer')
                peer_ref.receive(message, num, self.id)
        # print "Message delivered to everybody"

    def receive(self, message, num, sender):
        #if self.identifier == 2 and num == 2:
        #    sleep(5)        # Force a delay for the 3rd peer and second message
        self.queue.append(tuple([message, num, sender]))
        print self.queue
        if self.count < num:    # Get the biggest number as your timestamp
            self.count = num
        self.count += 1
        # print "My timestamp now is: ", self.count
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
                    people = self.host.lookup_url(peer_url[0],
                                                  'Lamport', 'Peer')
                    people.receive_ack(message, self.count, num)

    def receive_ack(self, message, ack, timestamp):
        acks = self.waiting[message, timestamp]
        acks.append(self.count)
        self.waiting[message, timestamp] = acks
        if self.count < ack:    # Get the biggest number as your timestamp
            self.count = ack
        self.deliver_queue()
        # print "And now my timestamp is:", self.count
        # We keep the message and the timestamp
        # print "I have this acks: ", len(acks)

    def deliver_queue(self):
        proceed = False
        try:
            while self.queue and not proceed:
                first = self.queue[0]
                message = first[0]  # Look if the first element has
                timestamp = first[1]   # All the acks
                sender = first[2]
                print "message:", message
                print "timestamp:", timestamp
                print "sender:", sender
                acks = self.waiting[message, timestamp]
                print "I have this acks: ", len(acks)
                if len(acks) == len(self.group.get_members()):
                    # If we can process the message
                    self.process_msg(message, sender)
                    del self.waiting[message, timestamp]
                    del self.queue[0]
                    # Leave the waiting queue
                else:
                    proceed = True
        except IndexError:
            print "There is no message in the waiting queue"

    def process_msg(self, message, sender):
        if message is not None:
            self.messages.append(message)
            print sender, ":", message+"\n:"


if __name__ == "__main__":
    global election_in_process
    election_in_process = False
    set_context()
    port = raw_input("Insert your port :")
    host = create_host('http://127.0.0.1:'+port)
    print host
    n_peer = raw_input("Write this peer's id:")
    kind = int(raw_input("Choose one type:\n1- Sequencer\t2- Lamport\n"))
    if kind == 1:
        peer = host.spawn(n_peer, Sequencer)
    elif kind == 2:
        peer = host.spawn(n_peer, Lamport)

    e1 = host.lookup_url('http://127.0.0.1:1280/group', 'Group', 'Group')
    peer.define_group(e1)
    print peer.join_me()

    exit = False
    while exit is False:
        msg = raw_input(": ")
        if msg == "test":
            peer.multicast_delay(msg)

        elif msg != "exit":
            peer.multicast_bully(msg, timeout=50)
        elif msg == "exit":
            exit = True

    print "----------------------- leaving the group---------------------"
    sleep(2)
    peer.to_leave()
    sleep(2)
    peer.print_messages()
    shutdown()
