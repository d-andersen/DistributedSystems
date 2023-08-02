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

from twisted.internet import protocol

from protocols import ClientProtocol, ServerProtocol


class ServerProtocolFactory(protocol.Factory):
    """
    Factory class responsible for accepting incoming connections by
    instantiating a ServerProtocol object representing the connection.
    """

    def __init__(self, controller):
        """
        Constructor. Saves a reference to the Controller
        """
        self.logger = logging.getLogger('factories.ServerProtocolFactory')
        self.controller = controller

    def buildProtocol(self, addr):
        """
        Creates an instance of the Server protocol and asks the
        Controller to add a reference to this instance to its dictionary
        of peers, using the IP of the peer + a '.S' suffix as a key. 
        This is to indicate that this is an incoming connection.
        The new Server protocol instance is then returned.
        """
        peer_id = addr.host
        peer = ServerProtocol(self, peer_id)
        self.controller.addPeer(peer_id, peer)

        self.logger.debug(f"DEBUG: Got connection from {addr.host}, {addr.port}")
        return peer

    def connectionMade(self, peer_id):
        self.controller.newConnectionMade(peer_id)

    def serverConnectionLost(self, peer_id):
        """
        Called by the Servel protocol that lost connection. All we do
        here is inform the Controller that we lost connection to a peer. 
        The Controller handles removing the peer from its dictionary of 
        peers.
        """
        self.controller.delPeer(peer_id)
    
    def recvMessage(self, message):
        """
        Passes received message to the controller to handle.
        """
        self.controller.handleIncomingMessage(message)


class ClientProtocolFactory(protocol.ClientFactory):
    """
    Factory class responsible for initiating outgoing connections by
    instantiating a ClientProtocol object representing the connection.
    """

    def __init__(self, controller):
        """
        Constructor. Saves a reference to the Controller
        """
        self.logger = logging.getLogger('factories.ClientProtocolFactory')
        self.controller = controller
    
    def startedConnecting(self, connector):
        addr = connector.getDestination()
        self.logger.debug(f"Started to connect to {addr.host}, {addr.port}")

    def buildProtocol(self, addr):
        """
        Creates an instance of the Client protocol and asks the
        Controller to add a reference to this instance to its dictionary
        of peers, using the IP of the peer + a '.C' suffix as a key. 
        This is to indicate that this is an outgoing connection.
        The new Client protocol instance is then returned.
        """
        peer_id = addr.host
        peer = ClientProtocol(self, peer_id)
        self.controller.addPeer(peer_id, peer)

        self.logger.debug(f"Connected to {addr.host}, {addr.port}")
        return peer

    def connectionMade(self, peer_id):
        self.controller.newConnectionMade(peer_id)

    def clientConnectionLost(self, connector, reason):
        """
        This should be called automatically by the Client protocol that
        lost connection. All we do here is inform the Controller that
        we lost connection to a peer. The Controller handles removing
        the peer from its dictionary of peers.
        """
        addr = connector.getDestination()
        self.logger.debug(f"Connected to {addr.host}, {addr.port}")
        self.controller.delPeer(addr.host)

    def clientConnectionFailed(self, connector, reason):
        """
        Called when a connection has failed to connect.
        We are not actively using or relying upon this.
        """
        addr = connector.getDestination()
        self.logger.debug(f"Connection to {addr.host}, {addr.port} failed. Reason: {reason}")

    def recvMessage(self, message):
        """
        Passes received message to the controller to handle.
        """
        self.controller.handleIncomingMessage(message)
