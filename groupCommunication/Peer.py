from pyactor.context import set_context, create_host, sleep, shutdown, interval
from pyactor.exceptions import TimeoutError
from random import uniform


class Peer(object):

    _tell = ['print_messages', 'set_sequencer', 'start_announcing', 'to_leave',
             'define_group', 'process_msg', 'keep_alive', 'receive',
             'print_count', 'start_delay', 'stop_delay']

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
        # For testing purposes
        self.delay = False

    def print_messages(self):
        print self.messages

    def get_url(self):
        if self.delay is True:
            sleep(5)
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
        self.group = None

    # Called if the last sequencer returns to the group without a join
    def kill_actor(self):
        host.stop_actor(self.id)
        shutdown()

    def keep_alive(self):
        self.group.announce(self.url, self.identifier)

    def start_delay(self):
        self.delay = True

    def stop_delay(self):
        self.delay = False


class Sequencer(Peer):

    _tell = Peer._tell + ['process_msg',
                          'receive',
                          'multicast',
                          'set_counter',
                          'set_n_messages', 'set_winner']
    _ask = Peer._ask + ['get_counter', 'initiate_election']
    _ref = Peer._ref + ['multicast', 'initiate_election', 'set_winner']

    def __init__(self):
        super(Sequencer, self).__init__()
        self.count = 0
        self.n_messages = 0

    def set_n_messages(self, number_of_messages):
        self.n_messages = number_of_messages

    def get_sequencer(self):
        return self.sequencer

    def set_sequencer(self, seq):
        self.sequencer = self.host.lookup_url(seq, 'Sequencer', 'Peer')

    def get_counter(self):
        if self.delay is True:
            sleep(20)
        self.count += 1
        # Sends the counter to the group to withstand sequencer failure
        self.group.set_count(self.count)
        print "GET_COUNTER: ", self.count
        return self.count - 1

    def set_counter(self, count):
        self.count = count
        print "Count set to: ", self.count

    def join_me(self):
        self.identifier = self.group.join(self.url)
        print "Joined successfully"
        print "My identifier is: ", self.identifier
        self.start_announcing()
        # Allow new peers to avoid waiting for messages sent before
        self.count = self.group.get_count()
        return self.identifier

    def receive(self, message, num, sender):
        print "Message received"
        print "COUNT: ", self.count
        print "NUM: ", num
        number = 0
        if self.proxy == self.sequencer:
            number = num
        else:
            number = num - self.count
        print "N_MESSAGES = ", self.n_messages
        print "NUMBER = ", number
        if self.n_messages == number:
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
        print "WAITING ", self.waiting
        print "MESSAGES ", self.messages

    def process_msg(self, message, sender):
        self.messages.append(message)
        self.n_messages += 1
        # Informs the group about the number of processed messages
        if self.proxy == self.sequencer:
            self.group.set_n_messages(self.n_messages)
        print sender, ":", message + "\n:"

    # If the sequencer has fallen a new one is chosen
    def initiate_election(self):
        print "--Initiating election"
        members = self.group.get_members()
        # Sort members by larger identifier
        sorted_members = sorted(members, key=lambda x: (-x[1]))
        winner_alive = False
        i = 0
        while winner_alive is False:
            winner_ref = self.host.lookup_url(sorted_members[i][0],
                                              'Sequencer', 'Peer')
            if winner_ref == self.proxy:
                winner_alive = True
            else:
                try:
                    # Test if the possible sequencer is alive
                    winner_ref.get_url(timeout=2)
                    winner_alive = True
                except TimeoutError:
                    print "Caught it"
            i += 1
        print "The winner is ", winner_ref
        self.set_winner(sorted_members, i-1, winner_ref)
        # election_in_process is False
        self.group.election_finished()

    # Tells every member the new sequencer and initialises it
    def set_winner(self, members, winner_index, winner_ref):
        counter = self.group.get_count()
        messages = self.group.get_n_messages()
        winner_url = members[winner_index][0]
        # Set the new sequencer to every member
        for member in members:
            if member[0] == self.url:
                # Sends the last counter and number of messages
                # to the new sequencer
                if self.proxy == winner_ref:
                    self.sequencer = winner_ref
                    self.count = counter
                    self.n_messages = messages
                else:
                    self.sequencer = winner_ref
                    winner_ref.set_counter(counter)
                    winner_ref.set_n_messages(messages)
                    winner_ref.set_sequencer(winner_url)
            else:
                peer_ref = self.host.lookup_url(member[0], 'Sequencer', 'Peer')
                peer_ref.set_sequencer(winner_url)
        self.group.set_sequencer(winner_url)

    # Supports sequencer failure using bully election algorithm
    def multicast(self, message):
        if self.sequencer == self.proxy:
            num = self.get_counter()
            print "SEQ: counter = ", num
        else:
            try:
                num = self.sequencer.get_counter(timeout=3)
            except TimeoutError:
                print "The sequencer has fallen"
                # In case more than one member detects the failure at the same
                # time
                sleep(uniform(0.1, 0.5))
                if self.group.get_election_in_process() is False:
                    # election_in_process = True
                    self.group.election_started()
                    self.initiate_election()
                    print "--The election has ended"
                    # self.multicast_bully(message)
                else:
                    # Wait while the election is happening
                    sleep(5)
                print "My seq is ", self.sequencer
                if self.sequencer == self.proxy:
                    num = self.get_counter()
                else:
                    num = self.sequencer.get_counter()
        if self.delay is True:
            print "WAITING..."
            sleep(5)
            print "AWAKEN"

        for peer_n in self.group.get_members(
        ):  # get_members returns url and identifier
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
                self.receive(message, num, self.id)
            else:
                print peer_n[0]
                peer_ref = self.host.lookup_url(peer_n[0], 'Lamport', 'Peer')
                peer_ref.receive(message, num, self.id)
        # print "Message delivered to everybody"

    def receive(self, message, num, sender):
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
            del self.waiting[message, num]
        else:   # Then, send ack to the rest of the group
            for peer_url in self.group.get_members():
                if peer_url[0] == self.url:
                    pass
                else:
                    people = self.host.lookup_url(peer_url[0],
                                                  'Lamport', 'Peer')
                    people.receive_ack(message, self.count, num)

    def receive_ack(self, message, ack, timestamp):
        # Force a delay for testing purposes
        if self.identifier == 1 and timestamp == 1:
            print "sleeping"
            sleep(5)
        acks = self.waiting[message, timestamp]
        acks.append(self.count)
        self.waiting[message, timestamp] = acks
        if self.count < ack:    # Get the biggest number as your timestamp
            self.count = ack
        self.deliver_queue()
        print "WAITING ", self.waiting
        print "MESSAGES ", self.messages
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
            print sender, ":", message + "\n:"


if __name__ == "__main__":
    set_context()
    port = raw_input("Insert your port :")
    host = create_host('http://127.0.0.1:' + port)
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
        if msg == "delay":
            print "Now there is a delay"
            peer.start_delay()
        elif msg == "recover":
            print "Delay removed. Working as usual"
            peer.stop_delay()
        elif msg == "exit":
            exit = True
        else:
            peer.multicast(msg, timeout=50)

    print "----------------------- leaving the group---------------------"

    sleep(2)
    peer.to_leave()
    sleep(2)
    peer.print_messages()
    shutdown()
