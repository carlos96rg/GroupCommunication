from pyactor.context import set_context, create_host, sleep, shutdown, interval


class Sequencer(object):

    def get_counter(self):
        print ""


class Peer(Sequencer):
    _tell = ['start_announcing', 'to_leave', 'define_group', 'process_message', 'keep_alive']
    _ask = ['join_me', 'get_id']
    _ref = ['join_me', 'start_announcing', 'keep_alive', 'to_leave', 'define_group']

    def __init__(self):
        self.group = None
        self.interval_reduce = None
        self.leader = None

    def define_group(self, group):
        self.group = group
        # self.group.print_swarm()

    def join_me(self):
        print self.id
        self.group.print_swarm()
        if self.group.join(self.id):
            print "Joined successfully"
        else:
            print "Failed joining"
        print "The group: ", self.group
        self.start_announcing()

    def get_id(self):
        return self.id

    def process_message(self):
        print self.group

    def start_announcing(self):
        print "Now I'm in"
        self.interval_reduce = interval(self.host, 3, self.proxy, "keep_alive")

    def to_leave(self):
        self.interval_reduce.set()
        self.group.leave(self)

    def keep_alive(self):
        print "Hi"
        self.group.announce(self.id)

if __name__ == "__main__":
    set_context()
    host = create_host('http://127.0.0.1:1679')
    peer1 = host.spawn('peer1', Peer)
    e1 = host.lookup_url('http://127.0.0.1:1280/group', 'Group', 'Group')
    peer1.define_group(e1)
    print "Group defined correctly"
    print "This is the group: ", e1
    peer1.join_me()
    sleep(10)
    shutdown()
