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
import sys

from prompt_toolkit import PromptSession
from prompt_toolkit.completion import WordCompleter
from prompt_toolkit.patch_stdout import patch_stdout
from twisted.internet import reactor


class Chat:
    """
    The Chat class is responsible for taking all input from the user and for
    parsing that input and passing along the parsed instructions to the
    Controller, which then further delegates or dispatches the necessary
    instructions.

    The Chat class is also manages the available commands and is able to show
    a help message with a list of commands to the user.
    """

    def __init__(self, controller):
        """
        Constructor

        NOTE that this runs in its own thread in order to not block the reactor
        (i.e. the main thread / main event loop)
        """
        self.logger = logging.getLogger('main.Chat')
        self.controller = controller
        self.state = 'GET_USER_NAME'

        # This is a dictionary of commands that the user can enter
        self.commands = {
            '-h': self.handleHelp,
            '-help': self.handleHelp,
            '-q': self.handleExit,
            '-quit': self.handleExit,
            '-exit': self.handleExit,
            '-delay': self.handleDelay,
            '-lu': self.handleListUsers,
            '-listusers': self.handleListUsers,
            '-lg': self.handleListGroups,
            '-listgroups': self.handleListGroups,
            '-finduser': self.handleFindUser,
            '-findgroup': self.handleFindGroup,
            '-creategroup': self.handleCreateGroup,
            '-joingroup': self.handleJoinGroup,
            '-leavegroup': self.handleLeaveGroup,
        }
        self.completer = WordCompleter([command for command in self.commands])

    async def takeUserName(self):
        """
        Initial input loop to take the user name from the user.
        """
        session = PromptSession()
        print("Welcome. Please enter a user name:")
        while True:
            with patch_stdout():
                line = await session.prompt_async('> ', completer=self.completer)
                line = line.strip()
                if len(line) == 0:
                    print("Note: The empty string is not a valid input")
                elif self.state == 'GET_USER_NAME':
                    self.validateUserName(line)

    async def takeInput(self, user_name):
        """
        Input loop. This method uses the prompt_toolkit prompt session, and is
        responsible for taking all input from the user and for the first step of
        processing, which entails determining whether the input is

          - 1) a program command
          - 2) a group (besides @all) or direct message
          - 3) none of the above, i.e. a message to @all
        """
        session = PromptSession()
        while True:
            with patch_stdout():
                line = await session.prompt_async(f'{user_name}> ', completer=self.completer)
                line = line.strip()
                if len(line) == 0:
                    print("Note: The empty string is not a valid input")
                elif line[0] == '-':
                    self.logger.debug(f"Chat parsing command: {line}")
                    self.parseCommand(line)
                elif line[0] == '@':
                    self.logger.debug(f"Chat parsing group: {line}")
                    self.parseAt(line)
                else:
                    group = 'all'
                    message = f"MSG @all {self.controller.user_name}> {line}"
                    self.logger.debug(f"Chat sending message: {message}")
                    reactor.callFromThread(self.controller.handleOutgoingMessage, group, message)

    def validateUserName(self, line):
        """
        Method to validate the user name entered by the user.

        Note that the only validation we make is that the user name doesn't contain a space ' ' or
        the at '@' symbol, since the latter is already a part of the used name, as well as used as
        the initial symbol for sending group and direct messages.
        :param line: Input line to validate
        """
        for token in [' ', '@']:
            if token in line:
                print("Error: Your user name may not contain the space ' ' or at '@' symbols")
                return
        reactor.callFromThread(self.controller.start, line)
        self.state = 'CHAT'
        sys.exit()

    def parseCommand(self, line):
        """
        Parses and validates the command entered by the user by making a lookup in the member dictionary
        'self.commands', which stores as values the appropriate method to call for that command. Note that
        some commands take an argument, which is also handled here.

        :param line: Line containing the command to parse
        """
        line = line.split(' ', 1)
        command = line[0]
        if command in self.commands:
            f = self.commands[command]
        else:
            print("Error: Unknown command {command}. Enter -h or -help for a list of commands")
            return
        if len(line) == 1:
            # command with no argument
            try:
                f()
            except TypeError:
                print(f"Error: Command {command} executed with wrong number of arguments")
                return
        elif len(line) == 2:
            # command with arguments
            try:
                f(line[1])
            except TypeError:
                print(f"Error: Command {command} executed with wrong number of arguments")
                return
        else:
            print(f"Error: Unknown command {command}. Enter -h or -help for a list of commands")
            return

    def parseAt(self, line):
        """
        Parses and validates the '@<target>' command entered by the user and prepends a message header,
        @<target> and user name to the message before passing it to the Controller.
        :param line: Line containing the command to parse
        """
        line = line.split(' ', 1)
        if len(line) == 1 or len(line[1].strip()) == 0:
            print("Note: Empty messages are not sent")
            return
        target = line[0][1:]
        if target not in self.controller.groups and target not in self.controller.users:
            print(f"Error: No group or user named {line[0]}")
            return
        if target in self.controller.groups and self.controller.user_name not in self.controller.groups[target]:
            print(f"Error: Cannot send message to group {target}, as you are not a member of that group")
            return
        message = f"MSG @{target} {self.controller.user_name}> {line[1]}"
        self.logger.debug(f"Chat sending message: {message}")
        reactor.callFromThread(self.controller.handleOutgoingMessage, target, message)

    def handleHelp(self):
        """
        Show the following help message to the user for how to use the program
        """
        print("Help. Showing list of commands")
        print("  -h  | -help                   Show this help message")
        print("  -q  | -quit | -exit           Exit the program")
        print("  -lu | -listusers              Show currently logged on users")
        print("  -lg | -listgroups             Show available groups")
        print("  -delay <timedelay> <message>  Sends a delayed message <message> delayed by")
        print("                                <timedelay> seconds. This is to allow for causal")
        print("                                order testing. Example usage:")
        print("                                  -delay 10 This message is delayed by 10 seconds")
        print("  -finduser <user>              Search for a user, where <user> is")
        print("                                the user name to search for")
        print("  -findgroup <group>            Search for a group, where <group> is")
        print("                                the group name to search for")
        print("  -creategroup <group>          Create and join a new group with name <group>")
        print("  -joingroup <group>            Join the group with name <group>")
        print("  -leavegroup <group>           Leave the group with name <group>")
        print("")
        print("  @<user>                       Sends a direct message to user with name <user>")
        print("  @<group>                      Sends a message to every user that is a member of <group>")

    def handleExit(self):
        """
        Notify the reactor that the user wants to terminate the program and then terminate this thread.
        """
        reactor.callFromThread(self.controller.terminate)
        sys.exit()

    def handleDelay(self, line):
        """
        Handles the -delay command allowing the user to test causal order delivery of messages.
        :param line: Input line to process.
        :return:
        """
        line = line.split(' ')
        try:
            delay = int(line[0])
        except ValueError:
            print("Error: The delay in the '-delay' command must be a number. For example: -delay 10 hi")
            return
        if not line[1]:
            print("Note: Empty messages are not sent")
            return
        msg = f"MSG @all {self.controller.user_name}> {line[1]}"
        args = ('all', msg, delay)
        reactor.callFromThread(self.controller.handleOutgoingMessage, *args)

    def handleListUsers(self):
        """
        Handles the -lu / -listusers command
        """
        reactor.callFromThread(self.controller.listUsers)

    def handleListGroups(self):
        """
        Handles the -lg / -listgroups command
        """
        reactor.callFromThread(self.controller.listGroups)

    def handleFindUser(self, line):
        """
        Handles the -finduser command allowing the user to search for a user by name
        :param line: Input line to process.
        """
        line = line.split(' ')
        if len(line) > 1:
            print("Error: the -finduser command takes exactly one argument")
            return
        user_name = line[0]
        reactor.callFromThread(self.controller.findUser, user_name)

    def handleFindGroup(self, line):
        """
        Handles the -findgroup command allowing the user to search for a group by name
        :param line: Input line to process.
        """
        line = line.split(' ')
        if len(line) > 1:
            print("Error: the -findgroup command takes exactly one argument")
            return
        group_name = line[0]
        if '@' in group_name:
            print("Error: The '@' symbol is not allowed in group names")
            return
        reactor.callFromThread(self.controller.findGroup, group_name)

    def handleCreateGroup(self, line):
        """
        Handles the -creategroup command allowing the user to create and join a new group
        :param line: Input line to process.
        """
        line = line.split(' ')
        if len(line) > 1:
            print("Error: the -creategroup command takes exactly one argument")
            return
        group_name = line[0]
        if '@' in group_name:
            print("Error: The '@' symbol is not allowed in group names")
            return
        if group_name in self.controller.groups:
            print("Error: Failed to create group {group_name}. Group already exists")
            return
        reactor.callFromThread(self.controller.createGroup, group_name)

    def handleJoinGroup(self, line):
        """
        Handles the -joingroup command allowing the user to join a group
        :param line: Input line to process.
        """
        line = line.split(' ')
        if len(line) > 1:
            print("Error: the -joingroup command takes exactly one argument")
            return
        group_name = line[0]
        if '@' in group_name:
            print("Error: The '@' symbol is not allowed in group names")
            return
        if group_name not in self.controller.groups:
            print("Error: Failed to join group {group_name}. Group doesn't exists")
            return
        reactor.callFromThread(self.controller.joinGroup, group_name)

    def handleLeaveGroup(self, line):
        """
        Handles the -leavegroup command allowing the user to leave a group
        :param line: Input line to process.
        """
        line = line.split(' ')
        if len(line) > 1:
            print("Error: the -leavegroup command takes exactly one argument")
            return
        group_name = line[0]
        if group_name == 'all':
            print("Error: You cannot leave the group 'all'")
            return
        if '@' in group_name:
            print("Error: The '@' symbol is not allowed in group names")
            return
        if group_name not in self.controller.groups:
            print("Error: Failed to leave group {group_name}. Group doesn't exists")
            return
        reactor.callFromThread(self.controller.leaveGroup, group_name)
