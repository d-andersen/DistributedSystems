## DM883 Distributed Systems Spring 2022 - Exam Project

#### Decentralized Chat

The aim of this project was to design and implement a distributed chat that supports multiple groups, as well as the following:

1. Listing users in a group chat and search for users based on their name.
2. Searching for groups based on their name.
3. Causal ordering of messages.
4. Message history.
5. An interactive user interface.

The system has been implemented as a peer-to-peer chat system using Python and the Twisted library. To make the project self-contained, we have used Docker networks to connect the clients.

#### How to run this project

Running this project requires Docker. A Dockerfile is included which should allow you to easily build the project. Navigate to the `artefact` folder and then run the command

```docker build -t dm883 .```

A docker-compose.yml file is also provided (note that this file requires Docker compose V2), which should allow you to easily start containers individually by running the command

```docker compose run --rm p0```

where `p0` to `p3` are valid.

If the above does not work, then the following command should

```docker run --rm -it -v "$(pwd):/dm883" dm883 python main.py```

In other words, to test this, you should

1. open two (or three or four) different terminals, navigate to the `artefact` folder in each

2. in the first terminal, run `docker compose run --rm p0`

3. in the second terminal, run `docker compose run --rm p1`

4. choose a user name in each terminal and you should be good to go!

## Authors

This project is a collaboration between

- Dennis Andersen - deand17
- Anders Nis Herforth Larsen - anla119
- Chiara Vimercati - chvim21
