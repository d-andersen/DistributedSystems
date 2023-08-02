"""
@author Dennis Andersen - deand17,
        Anders Nis Herforth Larsen - anla119,
        Chiara Vimercati - chvim21

@date 2022-05-31

DM883 Distributed Systems, Spring 2022

University of Southern Denmark

Group A03 Final Project. Topic: Decentralized Chat
"""
import asyncio
import copy
import json
import logging
import socket
import threading

from collections import defaultdict, deque

from twisted.internet import reactor

from causal import CausalOrderManager
from chat import Chat
from factories import ClientProtocolFactory
from protocols import DiscoverProtocol


class Controller:
    """
    The Controller has the overall responsibility of the program.

    Its main responsibility is being in charge of the shared data structures,
    that is peers/connections, user- and group names, and the message histories
    for each group.

    The Controller is also responsible for initiating the network DiscoverProtocol
    and for managing the exchange of data regarding data structures.
    """

    def __init__(self, udp_port, tcp_port):
        """
        Constructor
        :param udp_port: The upd port to listen on for the DiscoverProtocol
        :param tcp_port: The tcp port to listen on for incoming connections
        """
        self.logger = logging.getLogger('main.Controller')
        self.udp_port = udp_port
        self.tcp_port = tcp_port
        self.peers = {}
        self.user_name = ''
        self.users = defaultdict(list)
        self.groups = {'all': []}
        self.history_maxlen = 10
        self.histories = {}

        self.host_name = socket.gethostname()
        self.ip = socket.gethostbyname(self.host_name)

        self.discover = None

        self.CO_manager = CausalOrderManager(self.ip, self)
        self.chat = Chat(self)

        input_loop = threading.Thread(target=asyncio.run, args=(self.chat.takeUserName(),))
        input_loop.start()

    def start(self, user_name):
        """
        Finalize setup now that we have the user name of this peer.
        Afterwards, initiate the DiscoverProtocol
        :param user_name: The user name of this peer
        """
        self.user_name = f"{user_name}@{self.ip}"
        self.users[self.user_name].append('all')
        self.groups['all'].append(self.user_name)
        self.histories['all'] = deque(maxlen=self.history_maxlen)

        self.runDiscoverProtocol()
        input_loop = threading.Thread(target=asyncio.run, args=(self.chat.takeInput(self.user_name),))
        input_loop.start()
        print(f"{self.user_name} joined @all")

    def terminate(self):
        """
        Terminates the program
        """
        self.logger.debug("Controller stopping reactor, exiting")
        reactor.stop()

    def runDiscoverProtocol(self):
        """
        Initiate the DiscoverProtocol to discover other peers/containers in
        the network
        """
        if not self.discover:
            self.discover = DiscoverProtocol(controller=self, port=self.udp_port)
            reactor.listenMulticast(self.udp_port, self.discover, listenMultiple=True)
        else:
            self.discover.sendDiscoverMessage()

    def makeNewConnection(self, ip):
        """
        If we don't already have a connection to the peer with the
        ip given as the argument, then tell the reactor to make a
        new TCP connection to that peer using the ClientProtocolFactory
        """
        self.logger.debug(f"Controller initiating connection to ip: {ip}")
        self.logger.debug("Controller current list of peers:")
        for peer in self.peers:  # DEBUG
            self.logger.debug(f"    {peer}")

        if ip not in self.peers:
            reactor.connectTCP(ip, self.tcp_port, ClientProtocolFactory(self))

    def newConnectionMade(self, peer_id):
        """
        Acknowledge that a new connection has been made by returning
        the information stored in the data structures in this peer.
        """
        data = {
            'users': self.users,
            'groups': self.groups,
            'history': list(self.histories['all'])
        }
        self.logger.debug(f"Controller new connection made, sending DAT to {set([peer_id])}")
        message = f"DAT {json.dumps(data)}"
        self.CO_manager.send(message, set([peer_id]))

    def updateState(self, message):
        """
        Upon receiving a data message with header DAT, update the data structures
        of this peer with the information provided. In other words, the DAT comes
        with information about which users are online and which groups have been
        made, as well as a message history for the @all group.
        """
        data = json.loads(message)
        users = data['users']
        self.logger.debug(f"updateState: users: {users}")
        self.logger.debug(f"updateState: self.users: {self.users}")
        for user, groups in users.items():
            self.logger.debug(f"user: {user}, group: {groups}")
            if user not in self.users:
                self.logger.debug(f"Adding user {user} to users")
                self.users[user] = groups
                self.chat.completer.words.append(f"@{user}")
                print(f"{user} joined @all")

        groups = data['groups']
        for group, members in groups.items():
            # if group == self.user_name:
            #     continue
            if group not in self.groups:
                self.logger.debug(f"Adding group {group} with members {members} to groups")
                self.groups[group] = members
            elif not self.groups[group]:
                # group has no members, so remove it
                del self.groups[group]
            else:
                # we already know about this group, but there might be a
                # discrepancy between the list of members that we receive
                # vs. our own list of members for that group
                for member in members:
                    if member not in self.groups[group]:
                        self.groups[group].append(member)

        history = data['history']
        for message in history:
            if message not in self.histories['all']:
                self.histories['all'].append(message)
                print(message)

    def addPeer(self, peer_id, peer):
        """
        Add a peer to keep track of.
        """
        self.logger.debug(f"Controller added {peer_id} to peers")
        self.peers[peer_id] = peer
        self.CO_manager.addPeer(peer_id)

    def delPeer(self, peer_id):
        """
        Delete a peer, if present, that we are keeping track of.
        """
        if peer_id in self.peers:
            self.logger.debug(f"Controller deleted {peer_id} from peers")
            del self.peers[peer_id]
        try:
            peer_user_name = [user for user in self.users if peer_id in user][0]
            groups_where_peer_is_member = self.users[peer_user_name]
            self.logger.debug(f"peer_user_name: {peer_user_name}")
            self.logger.debug(f"groups where peer is member: {groups_where_peer_is_member}")
            for group in groups_where_peer_is_member:
                self.groups[group].remove(peer_user_name)
            self.chat.completer.words.remove(f"@{peer_user_name}")
            del self.users[peer_user_name]
            self.CO_manager.delPeer(peer_id)
            print(f"{peer_user_name} left @all")
        except IndexError as e:
            print(f"An unexpected error occured: {e}")

    def handleIncomingMessage(self, message):
        """
        Handle incoming message by passing it to the causal order manager.
        :param message: Message to process
        """
        self.CO_manager.recvHandler(message)

    def handleOutgoingMessage(self, target, message, delay=0):
        """
        Send the message to the appropriate peers.
        :param target: Target ip of receiver
        :param message: Message to send
        :param delay: An optional delay. To enable testing of causal order
        """
        if '@' in target:
            # target is a single user, i.e. message is a PM
            Dests = set([target[1:].split('@')[1]])
        else:
            # target is a group
            Dests = set([member.split('@')[1] for member in self.groups[target]])

        # Don't send a message to ourselves
        Dests.discard(self.ip)

        if not Dests:
            # if there are no recipients, then just return
            return
        self.recordMessage(copy.deepcopy(message))
        self.CO_manager.send(message, Dests, delay=delay)

    def deliverMessage(self, message):
        """
        Determine how to deliver the message.
        :param message: Message to deliver
        """
        self.recordMessage(copy.deepcopy(message))
        header = message[0:3]
        message = message[4:]
        if header == 'MSG':
            print(message)
        elif header == 'DAT':
            self.updateState(message)
        elif header in ['CRG', 'JNG', 'LVG', 'AJG', 'ALG']:
            self.updateGroup(header, message)

    def recordMessage(self, message):
        """
        Record the message for the appropriate group
        :param message: Message to record
        """
        header = message[0:3]
        if header == 'MSG':
            message = message[4:]
            first_space = message.find(' ')
            target = message[1:first_space]
            if target in self.histories:
                self.histories[target].append(message)

    def listUsers(self):
        """
        Show a list of the users that we are currently aware of
        """
        print("Listing users")
        for user in self.users:
            print(f"  - {user}")

    def findUser(self, user_name):
        """
        Search our local list of users for the given user name
        :param user_name: The user name to search for
        """
        matches = []
        for user, groups in self.users.items():
            if user_name in user:
                matches.append((user, groups))
        print(f"Searching for user: {user_name}")
        if matches:
            print(f"    User                            Groups")
            for match in matches:
                print(f"  - {match[0]:<32}{match[1]}")
        else:
            print(f"    No user with {user_name} found")

    def listGroups(self):
        """
        Show a list of the currently available groups to the user
        """
        print("Listing groups")
        for group, members in self.groups.items():
            print(f"  - {group}: {members}")

    def findGroup(self, group_name):
        """
        Search our local list of groups for the given group name
        :param group_name: The group name to search for
        :return:
        """
        matches = []
        for group, members in self.groups.items():
            if group_name in group:
                matches.append((group, members))
        print(f"Searching for group: {group_name}")
        if matches:
            print(f"    Group                           Users")
            for match in matches:
                print(f"  - {match[0]:<32}{match[1]}")
        else:
            print(f"    No group with name {group_name} found")

    def createGroup(self, group):
        """
        Creates a new group with the name given in the argument unless that group already exists,
        in which case an error message is shown to the user. If a new group is created, all other
        peers are informed via what is essentially a broadcast, by sending an information message
        with header CRG on all open connections.
        :param group: The name of the group to be created
        """
        if group not in self.groups:
            self.groups[group] = [self.user_name]
            self.chat.completer.words.append(f"@{group}")

            if group not in self.users[self.user_name]:
                self.users[self.user_name].append(group)
            if group not in self.histories:
                self.histories[group] = deque(maxlen=self.history_maxlen)

            data = {
                'group': group,
                'members': [self.user_name],
                'group_history': []
            }
            message = f"CRG {json.dumps(data)}"
            self.handleOutgoingMessage('all', message)

            print(f"{self.user_name} created and joined group {group}")
        else:
            print(f"Error: Group {group} already exists")

    def joinGroup(self, group):
        """
        Joins the group with the name given in the argument unless that group doesn't exist or the
        user is already a member of that group, in which case an error message is shown to the user.
        If the user joins the group, all other peers are informed via what is essentially a broadcast,
        by sending an information message with header JNG on all open connections.
        :param group: The name of the group to join
        :return:
        """
        if group in self.groups:
            if self.user_name not in self.groups[group]:
                self.groups[group].append(self.user_name)
                self.chat.completer.words.append(f"@{group}")

                if group not in self.users[self.user_name]:
                    self.users[self.user_name].append(group)
                if group not in self.histories:
                    self.histories[group] = deque(maxlen=self.history_maxlen)

                data = {
                    'group': group,
                    'members': [self.user_name],
                    'group_history': []
                }
                message = f"JNG {json.dumps(data)}"
                self.handleOutgoingMessage('all', message)

                print(f"{self.user_name} joined group {group}")
            else:
                print(f"Error: Failed to join group {group}: You are already a member of {group}")
        else:
            print(f"Error: Failed to join group {group}: No group with that name")

    def leaveGroup(self, group):
        """
        Leaves the group with the name given in the argument if that group exist and the user is a member,
        otherwise an error message is shown to the user. If the user leaves the group, all other peers
        are informed via what is essentially a broadcast, by sending an information message with header LVG
        on all open connections.

        Note that when the last member of a group leaves, the group is deleted and is no longer available to
        join. It is not allowed to leave the @all group.
        :param group: The name of the group to leave.
        :return:
        """
        if group in self.groups:
            if self.user_name in self.groups[group]:
                self.groups[group].remove(self.user_name)
                self.chat.completer.words.remove(f"@{group}")

                if not self.groups[group]:
                    del self.groups[group]
                    if group in self.histories:
                        # if the group is empty, we don't need to keep track of its history
                        del self.histories[group]

                if group in self.users[self.user_name]:
                    self.users[self.user_name].remove(group)

                data = {
                    'group': group,
                    'members': [self.user_name],
                    'group_history': []
                }
                message = f"LVG {json.dumps(data)}"
                self.handleOutgoingMessage('all', message)

                print(f"{self.user_name} left group {group}")
            else:
                print(f"Error: Failed to leave group {group}: You are not a member of {group}")
        else:
            print(f"Error: Failed to leave group {group}: No group with that name")

    def handleCreateGroupBCast(self, group, members, group_history):
        """
        Updates the local data structures of this peer upon receiving a message with header
        CRG that a user has created a new group
        :param group: The name of the group created
        :param members: The creator of the group
        :param group_history: The history of the group. This is an empty list for a newly created group
        """
        user = members[0]
        if group not in self.groups:
            self.groups[group] = members

        if user in self.users:
            self.users[user].append(group)

        if group not in self.histories:
            self.histories[group] = deque(maxlen=self.history_maxlen)

        print(f"{user} created and joined group {group}")

    def handleJoinGroupBCast(self, group, members, group_history):
        """
        Updates the local data structures of this peer upon receiving a message with header
        JNG that a user has joined a group
        :param group: The name of the group created
        :param members: The creator of the group
        :param group_history: The history of the group. This is an empty list for a newly created group
        """
        user = members[0]
        if group in self.groups and user not in self.groups[group]:
            self.groups[group].append(user)
        else:
            self.groups[group] = members

        if user in self.users and group not in self.users[user]:
            self.users[user].append(group)

        if self.user_name in self.groups[group]:
            data = {
                'group': group,
                'members': members,
                'group_history': list(self.histories[group])
            }
            message = f"AJG {json.dumps(data)}"
            self.handleOutgoingMessage(user, message)

        print(f"{user} joined group {group}")

    def handleLeaveGroupBCast(self, group, members, group_history):
        """
        Updates the local data structures of this peer upon receiving a message with header
        LVG that a user has left a group

        Note that if the group is empty after processing this message, the group is deleted
        and is no longer available to join.
        :param group: The name of the group created
        :param members: The creator of the group
        :param group_history: The history of the group. This is an empty list for a newly created group
        """
        user = members[0]
        if group in self.groups:
            if user in self.groups[group]:
                self.groups[group].remove(user)

            if not self.groups[group]:
                # group has no members, so remove it
                del self.groups[group]

        if user in self.users and group in self.users[user]:
            self.users[user].remove(group)

        print(f"{user} left group {group}")

    def handleJoinGroupAck(self, group, members, group_history):
        """
        Handle updating the message history of the group given in the argument and present
        the message history to the user.

        Known issue: When joining a group, upon receiving the message history for the group,
        the messages are not always displayed immediately. We suspect this may be a buffering
        issue in either the Twisted module or the prompt_toolkit module, however, we have not
        been able to pin down this minor bug.
        :param group: The name of the group created
        :param members: The creator of the group
        :param group_history: The history of the group. This is an empty list for a newly created group
        """
        if self.user_name in self.groups[group]:
            for msg in group_history:
                if msg not in self.histories[group]:
                    self.histories[group].append(msg)
                    print(msg)

    def updateGroup(self, header, message):
        """
        Dispatcher for group update messages.
        :param header: Header specifying type of message
        :param message: Update message
        """
        dispatch = {
            'CRG': self.handleCreateGroupBCast,
            'JNG': self.handleJoinGroupBCast,
            'LVG': self.handleLeaveGroupBCast,
            'AJG': self.handleJoinGroupAck,
        }
        data = json.loads(message)
        group = data['group']
        members = data['members']
        group_history = data['group_history']
        dispatch[header](group, members, group_history)
