"""
@author Dennis Andersen - deand17,
        Anders Nis Herforth Larsen - anla119,
        Chiara Vimercati - chvim21

@date 2022-06-02

DM883 Distributed Systems, Spring 2022

University of Southern Denmark

Group A03 Final Project. Topic: Decentralized Chat
"""
import logging
import socket

from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ServerEndpoint

from factories import ServerProtocolFactory
from controller import Controller

# -----------------------------------------------------------------------------
# CONSTANTS
# -----------------------------------------------------------------------------
UDP_PORT = 9999
TCP_PORT = 8000


# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logger = logging.getLogger('main')
logger.setLevel(logging.DEBUG)
# File logging
log_fh = logging.FileHandler(f'{socket.gethostbyname(socket.gethostname())}.log')
log_fh.setLevel(logging.DEBUG)
log_file_formatter = logging.Formatter('%(asctime)s %(name)-31s %(levelname)-8s %(message)s')
log_fh.setFormatter(log_file_formatter)
logger.addHandler(log_fh)

# Stream logging
# log_ch = logging.StreamHandler()
# log_ch.setLevel(logging.DEBUG)
# log_stream_formatter = logging.Formatter('%(name)-31s: %(levelname)-8s %(message)s')
# log_ch.setFormatter(log_stream_formatter)
# logger.addHandler(log_ch)


def main():
    # Create an instance of ChatInput, which will act as our controller
    # and connect it to stdio
    controller = Controller(UDP_PORT, TCP_PORT)

    # Register the TCP_PORT on which we want to listen for incoming TCP
    # connections and start listening
    endpoint = TCP4ServerEndpoint(reactor, TCP_PORT)
    endpoint.listen(ServerProtocolFactory(controller))

    # Start the Twisted event-loop
    # reactor.registerGApplication(controller.app)
    reactor.run()


if __name__ == '__main__':
    main()