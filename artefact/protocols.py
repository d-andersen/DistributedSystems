"""
@author Dennis Andersen - deand17,
        Anders Nis Herforth Larsen - anla119,
        Chiara Vimercati - chvim21

@date 2022-05-28

DM883 Distributed Systems, Spring 2022

University of Southern Denmark

Final Project

Topic: Decentralised Chat
"""
import logging
import random

from twisted.internet import protocol, reactor


class DiscoverProtocol(protocol.DatagramProtocol):
    """
    Discover protocol

    This is used to discover and connect to other peers on the network 
    and works in the following way. When the program is started, we 
    broadcast to every unknown peer in the network a datagram with a 
    'DISC' "header" and a random floating pointer number as "payload". 
    When a datagram is received, we check for the 'DISC' header and then, 
    if the datagram originates from a peer different from ourselves,
    we compare the random numbers. If our random number is the smallest,
    we initiate a TCP connection to the other peer, otherwise, we reply
    with our random number, since the other peer might not have received
    our initial broadcast datagram.

    We might need to include some kind of callback to do a re-broadcast
    later in case our list of peers is empty.
    """
    noisy = False

    def __init__(self, controller, port):
        """
        Constructor. Saves a reference to the Controller, registers
        the port at which to send UDP datagrams, and generates an initial
        random real number in [0,1).
        """
        self.logger = logging.getLogger('protocols.DiscoverProtocol')
        self.controller = controller
        self.port = port
        self.random_real = random.random()

    def startProtocol(self):
        """
        Initialize the protocol and enable broadcasting.
        Then wait a small random amount of time before initiating the
        broadcast. This is to prevent that when multiple peers/containers
        are brought online simultaneously, they do not all broadcast
        at more or less the same instant, which should help to spread out
        the network traffic load. In addition, it also gives peers a
        chance to get up and running.
        """
        self.transport.setBroadcastAllowed(True)
        # NOTE The random interval has been chosen arbitrarily and might need 
        # tuning
        reactor.callLater(random.uniform(1, 5), self.sendDiscoverMessage)

        # NOTE This was intended as a sort of failsafe in the event that
        # no other peers were found initially, then we would try again after
        # a short delay, however, this shouldn't be needed. However, we might
        # need to consider including a way for the user to manually request
        # running the Discover protocol in order to reconnect.
        # reactor.callLater(random.randint(10, 15), self.sendDiscoverMessage)

    def sendDiscoverMessage(self, ip="<broadcast>"):
        """
        Pass a discovery datagram with the header 'DISC' followed by
        our randomly generated real number in [0,1) to the transport layer
        for broadcast.

        NOTE I'm unsure whether it's necessary to generate a new random 
        number whenever we call this, or whether the initially generated
        number is sufficient
        """
        # self.random_real = random.random()  # this may be unnecessary ?
        discover_msg = f"DISC {self.random_real}"
        self.logger.debug(f"{self.controller.host_name}@{self.controller.ip} sent: " + discover_msg)
        discover_msg = discover_msg.encode()
        self.transport.write(discover_msg, (ip, self.port))

    def datagramReceived(self, datagram, addr):
        """
        Receive a datagram from the transport layer and decode it. If
        it contains our 'DISC' header, extract the random number and
        compare it to our own. If our random number is less than that
        of the sender, we ask the Controller to initiate a connection
        to the sender. If not, we return a discovery datagram to the
        sender, in case the sender has joined at a later time and thus
        has not received our random number when it was initially 
        broadcast.
        """
        datagram = datagram.decode()
        self.logger.debug(f"{self.controller.host_name}@{self.controller.ip} recv: " + datagram + f" from {addr}")
        if datagram[:4] == 'DISC' and addr[0] != self.controller.ip:
            other_random_real = float(datagram[5:])
            if self.random_real < other_random_real:
                self.logger.debug(f"{self.controller.host_name}@{self.controller.ip} random_real: {self.random_real} ?<? {other_random_real} :other_random_real")
                self.logger.debug(f"{self.controller.host_name}@{self.controller.ip} initiating connection ...")
                # self.controller.makeNewConnection(addr[0])
                reactor.callFromThread(self.controller.makeNewConnection, addr[0])
            else:
                self.sendDiscoverMessage(ip=addr[0])


class ServerProtocol(protocol.Protocol):
    """
    Server protocol

    Used to listen to and receive connection requests from other peers.
    Called by ServerProtocolFactory each time a peer makes a connection
    to this host
    """

    def __init__(self, factory, peer_id):
        """
        Constructor. Saves a reference to its factory, as well as the
        id used to find itself in the dictionary of peers
        """
        self.logger = logging.getLogger('protocols.ServerProtocol')
        self.factory = factory
        self.peer_id = peer_id

    def connectionMade(self):
        self.factory.connectionMade(self.peer_id)

    def connectionLost(self, reason):
        """
        If we lose connection, inform the factory and let it handle the
        event.
        """
        self.factory.serverConnectionLost(self.peer_id)

    def dataReceived(self, data):
        """
        Receives a message as bytes from the transport layer, decodes
        the message and passes it to the Controller.
        """
        # self.logger.debug("ServerProtocol data received")
        # data = data.decode()
        # n = int(data[:5])
        # data = data[5:]
        # while len(data) > n:
        #     self.logger.debug("message was longer than expected")
        #     message = data[0:n]
        #     self.logger.debug(f"passing along received message: {message}")
        #     self.factory.recvMessage(message)
        #     data = data[n:]
        #     n = int(data[:5])
        #     data = data[5:]
        # self.logger.debug(f"passing along received message: {data}")
        # self.factory.recvMessage(data)
        self.factory.recvMessage(data.decode())
        # stdout.write(data.decode())

    def sendMessage(self, data):
        """
        Encodes the message as bytes before sending it off to the
        transport layer.
        """
        # self.transport.write(data.encode())
        # n = len(data)
        # data = f'{n:05}' + data
        # self.logger.debug(f"ServerProtocol sending {data}")
        self.transport.write(data.encode())


class ClientProtocol(protocol.Protocol):
    """
    Client protocol

    Used to make a connection to other peers. Called by 
    ClientProtocolFactory each time we want to make a new connection
    """
    def __init__(self, factory, peer_id):
        """
        Constructor. Saves a reference to its factory, as well as the
        id used to find itself in the dictionary of peers
        """
        self.logger = logging.getLogger('protocols.ClientProtocol')
        self.factory = factory
        self.peer_id = peer_id

    def connectionMade(self):
        self.factory.connectionMade(self.peer_id)

    def connectionLost(self, reason):
        """
        If we lose connection, the clientConnectionLost method on the 
        factory should automatically be called, so we do nothing here.
        """
        pass

    def dataReceived(self, data):
        """
        Receives a message as bytes from the transport layer, decodes
        the message and passes it to the Controller.
        """
        # self.logger.debug("ClientProtocol data received")
        # data = data.decode()
        # n = int(data[:5])
        # data = data[5:]
        # while len(data) > n:
        #     self.logger.debug("message was longer than expected")
        #     message = data[0:n]
        #     self.logger.debug(f"passing along received message: {message}")
        #     self.factory.recvMessage(message)
        #     data = data[n:]
        #     n = int(data[:5])
        #     data = data[5:]
        # self.logger.debug(f"passing along received message: {data}")
        # self.factory.recvMessage(data)
        self.factory.recvMessage(data.decode())
        # stdout.write(data.decode())

    def sendMessage(self, data):
        """
        Encodes the message as bytes before sending it off to the
        transport layer.
        """
        # n = len(data)
        # data = f'{n:05}' + data
        # self.logger.debug(f"ClientProtocol sending {data}")
        # self.transport.write(data.encode())
        self.transport.write(data.encode())
