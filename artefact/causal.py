"""
@author Dennis Andersen - deand17,
        Anders Nis Herforth Larsen - anla119,
        Chiara Vimercati - chvim21

@date 2022-05-28

DM883 Distributed Systems, Spring 2022

University of Southern Denmark

Final Project

Topic: Decentralised Chat

References:
    Ajay D. Kshemkalyani and Mukesh Singhal. 2008. 
    Distributed Computing: Principles, Algorithms, and Systems (1st. ed.),
    Ch. 6, "Message ordering and group communication", pp. 206--215.
    Cambridge University Press, USA.
"""
import copy
import json
import logging
import threading

from dataclasses import dataclass

from twisted.internet import reactor


@dataclass(init=True, repr=True, eq=True, order=False, unsafe_hash=False, frozen=False)
class CausalMessageEntry:
    """
    Data class representing a causal message entry, which are used by the
    local log LOG_j as well as piggy-backed on messages in the O_M set.
    
    NOTE Note that frozen is set to False, meaning this data class is mutable.
    This is necessary, because the algorithm performs modifications to the
    set of tracked destinations Dests. However, fields i and clock_i are
    can be considered constant, as they are never changed by the algorithm.
    """
    i: str
    clock_i: int
    Dests: set

    def __hash__(self):
        """
        Explicitly make hashable, even though frozen is set to False (i.e.
        this data class is mutable). This is required in order to use 
        instances of this class in sets.
        """
        return hash((self.i, self.clock_i))

    def __repr__(self):
        return f"CME({self.i}, {self.clock_i}, {self.Dests})"


class CausalOrderManager:
    """
    Kshemkalyani-Singhal (KS) algorithm for optimal causal ordering of
    messages. (Algorithm 6.3, p. 211 in 'Redbook')

    This responsibility of the CausalOrderManager is enforcing causal order of all messages
    sent and received.
    """
    def __init__(self, own_id, controller):
        """
        Constructor.
        """
        self.logger = logging.getLogger('causal.CausalOrderManager')
        self.j = own_id
        self.controller = controller
        self.clock_j = 0
        self.SR_j = {own_id: 0}
        self.LOG_j = set([CausalMessageEntry(own_id, 0, set())])
        self.condition = threading.Condition()

    def send(self, M, Dests, delay=0):
        # Using a Condition is similar to using a lock. This should ensure that
        # this method executes atomically
        with self.condition:
            # (1a)
            self.clock_j = self.clock_j + 1
            # (1b)
            for d in Dests:
                O_M = copy.deepcopy(self.LOG_j)
                for o in O_M:
                    # Do not propagate information about indirect dependencies
                    # that are guaranteed to be transitively satisfied when
                    # dependencies of M are satisfied
                    if d not in o.Dests:
                        o.Dests = o.Dests.difference(Dests)
                    if d in o.Dests:
                        o.Dests = o.Dests.difference(Dests).union([d])

                for o_st in O_M:
                    s = o_st.i
                    t = o_st.clock_i
                    if len(o_st.Dests) == 0 and self.newerEntryExists(s, t, O_M):
                        # do not propagate older entries for which Dests is Ø
                        O_M = O_M.difference([o_st])
                
                # print(f"DEBUG: sending ({self.j}, {self.clock_j}, {M}, {Dests}, {O_M})")
                self.logger.debug(f"sending ({self.j}, {self.clock_j}, {M}, {Dests}, {O_M})")
                packed_message = self.packMessage(self.j, self.clock_j, M, Dests, O_M)
                # if self.clock_j == 2:
                #     reactor.callLater(10, self.controller.peers[d].sendMessage, packed_message)
                # elif self.clock_j == 3:
                #     reactor.callLater(5, self.controller.peers[d].sendMessage, packed_message)
                # else:
                #     # reactor.callLater(0, self.controller.peers[d].sendMessage, packed_message)
                #     reactor.callFromThread(self.controller.peers[d].sendMessage, packed_message)
                reactor.callLater(delay, self.controller.peers[d].sendMessage, packed_message)

            # (1c) Do not store information about indirect dependencies that
            # are guaranteed to be transitively satisfied when dependencies of
            # M are satisfied
            for l in self.LOG_j:
                l.Dests = l.Dests.difference(Dests)
            # purge l in LOG_j if l.Dests = Ø
            self.purgeNullEntries()
            # (1d)
            self.LOG_j = self.LOG_j.union(
                [CausalMessageEntry(self.j, self.clock_j, Dests)])
            self.condition.notify_all()

    def recvHandler(self, message):
        """
        Delegator method. Since a received message may have to wait, we
        process the message in a new thread and use the Condition construct 
        to allow this thread to non-blockingly wait for a certain condition
        to become true. Thus the Condition object also functions as a lock,
        such that the processing of the message should happen atomically, as
        specified in the original algorithm. See the recv method for more
        details.
        """
        unpacked_message = self.unpackMessage(message)
        worker = threading.Thread(target=self.recv, args=(self.condition, *unpacked_message))
        worker.start()

    def recv(self, condition, k, t_k, M, Dests, O_M):
        # Using a Condition is similar to using a lock. This should ensure that
        # this method executes atomically. In addition, we need this such that
        # we can wait to be notified when a message can be delivered, i.e.
        # when messages sent causally before M are delivered
        with condition:
            # (2a) Delivery Condition: ensure that messages sent causally
            # before M are delivered
            for o_mt_m in O_M:
                m = o_mt_m.i
                t_m = o_mt_m.clock_i
                if self.j in o_mt_m.Dests and not t_m <= self.SR_j[m]:
                    print(f"DEBUG: j: '{self.j}' in o_m,t_m.Dests: {o_mt_m.Dests} "
                          f"and this message needs to wait: {M}")
                    # Wait for messages sent causally before M to be delivered
                    while not t_m <= self.SR_j[m]:
                        condition.wait_for(lambda: t_m <= self.SR_j[m])
            # (2b) Deliver M
            # print(f"DEBUG: Delivering message {M}", end='')
            self.controller.deliverMessage(M)
            self.SR_j[k] = t_k
            # (2c) delete the now redundant dependency of message represented
            # by o_m,t_m sent to j
            O_M.add(CausalMessageEntry(k, t_k, Dests))
            for o_mt_m in O_M:
                o_mt_m.Dests = o_mt_m.Dests.difference(self.j)
            # (2d) Merge O_M and LOG_j by eliminating all redundant entries.
            # Implicitly track "already delivered" and "guaranteed to be
            # delivered in CO" messages
            marked_o_mts = []
            marked_l_stprimes = []
            LOG_j_as_list = [(elem.i, elem.clock_i) for elem in self.LOG_j]
            O_M_as_list = [(elem.i, elem.clock_i) for elem in O_M]
            for o_mt in O_M:
                m = o_mt.i
                t = o_mt.clock_i
                for l_stprime in self.LOG_j:
                    s = l_stprime.i
                    tprime = l_stprime.clock_i
                    if s == m:
                        if t < tprime and (s, t) not in LOG_j_as_list:
                            # l_s,t had been deleted or never inserted as
                            # l_s,t.Dests = Ø in the causal past
                            marked_o_mts.append(o_mt)
                        if tprime < t and (m, tprime) not in O_M_as_list:
                            # o_m,t' not in O_M because l_s,t' had become Ø
                            # at another process in the causal past
                            marked_l_stprimes.append(l_stprime)
            # delete entries about redundant information
            for marked in marked_o_mts:
                O_M.discard(marked)
            for marked in marked_l_stprimes:
                self.LOG_j.discard(marked)

            marked_o_mts = []
            for l_stprime in self.LOG_j:
                s = l_stprime.i
                tprime = l_stprime.clock_i
                for o_mt in O_M:
                    m = o_mt.i
                    t = o_mt.clock_i
                    if s == m and tprime == t:
                        # delete destinations for which Delivery Condition is
                        # satisfied or guaranteed to be satisfied as per o_m,t
                        l_stprime.Dests = l_stprime.Dests.intersection(o_mt.Dests)
                        # information has been incorporated in l_s,t', so mark
                        # for removal since we cannot change the size of O_M
                        # without invalidating the iterator
                        marked_o_mts.append(o_mt)
            # We then remove l_s,t' post iterating
            for marked in marked_o_mts:
                O_M.discard(marked)
            # merge non-redundant information of O_M into LOG_j
            self.LOG_j = self.LOG_j.union(O_M)
            # (2e) Purge older entries l for which l.Dests = Ø is implicitly inferred
            self.purgeNullEntries()
            condition.notify_all()

    def newerEntryExists(self, s, t, X):
        """
        Checks the set X (which may be either the local log LOG_j or a set
        O_M piggy-backed on a message) to see if a newer CausalMessageEntry
        exists such that the id's are the same, but the entry in X has a
        newer timestamp.
        """
        for x in X:
            t_prime = x.clock_i
            if s == x.i and t < t_prime:
                return True
        return False

    def purgeNullEntries(self):
        """
        Purges the local log LOG_j for older entries l for which l.Dests = Ø 
        is implicitly inferred.
        """
        marked_l_sts = []
        for l_st in self.LOG_j:
            s = l_st.i
            t = l_st.clock_i
            if len(l_st.Dests) == 0 and self.newerEntryExists(s, t, self.LOG_j):
                marked_l_sts.append(l_st)
        for marked in marked_l_sts:
            self.LOG_j.discard(marked)

    def packMessage(self, m, clock_m, M, Dests, O_M):
        """
        Packs a message as a json string
        """
        return json.dumps([m, clock_m, M, list(Dests),
                           list(map(lambda cme: [cme.i, cme.clock_i, list(cme.Dests)], O_M))])

    def unpackMessage(self, message):
        """
        Unpacks a json string and reshapes it as a tuple to use as input for
        the recv method
        """
        unpacked = json.loads(message)
        return (unpacked[0], unpacked[1], unpacked[2], set(unpacked[3]),
                set(list(map(lambda e: CausalMessageEntry(e[0], e[1], set(e[2])), unpacked[4]))))

    def addPeer(self, peer_id):
        """
        Include another peer for which to keep track of the causal order of
        messages
        """
        self.SR_j[peer_id] = 0
        self.LOG_j.add(CausalMessageEntry(peer_id, 0, set()))

    def delPeer(self, peer_id):
        """
        Removes a peer from causal order tracking
        This is required to enable containers to rejoin with the same ip

        NOTE This has only been subject to minimal testing. It SEEMS to work,
        but there may be hidden bugs which would only arise as testing runs
        for longer and with many more peers.
        """
        del self.SR_j[peer_id]
        marked_cmes = []
        for cme in self.LOG_j:
            if peer_id == cme.i or peer_id in cme.Dests:
                marked_cmes.append(cme)
        for cme in marked_cmes:
            self.LOG_j.discard(cme)
        self.LOG_j.add(CausalMessageEntry(self.controller.ip, 0, set()))
