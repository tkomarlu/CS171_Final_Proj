from ballot import Ballot
from network import sendMessage, QUORUM_SIZE, NUM_OF_SERVERS
import threading
import time

class Paxos:
    depth = 0
    proposalVal = None
    promises = []
    accepts = 0
    onDecison = None
    onAccept = None
    isLeader = False
    sockets = None
    activeLinks = {'1': True, '2': True, '3': True, '4': True, '5': True}
    
    def __init__(self, sockets, myId, onDecision, onAccept):
        self.onDecison = onDecision
        self.onAccept = onAccept
        self.myId = str(myId)
        self.ballotNum = Ballot(0,self.myId,0)
        self.acceptBal = Ballot(0,self.myId,0)
        self.acceptVal = None
        self.sockets = sockets
        self.lock = threading.Lock()
        self.depthLock = threading.Lock()
        
    #PHASE I: Prepare
    #Proposer    
    def prepareProposal(self, operation, timestamp):
        print("Starting Leader Election -- Broadcasting PREPARE", flush=True)
        self.isLeader = False
        self.ballotNum = Ballot(self.ballotNum.seqNum+1, self.myId, self.depth)
        self.proposalVal = operation
        self.promises = []
        self.accepts = 0
        self.isAccepted = True #If we are a leader and we get a request to accept we deny because we accept our value
        msg = {
            'type': 'PREPARE',
            'ballot':self.ballotNum,
            'timestamp':timestamp
        }
        for x in range(1, NUM_OF_SERVERS+1):
            if(str(x) != self.myId):
                sendMessage(self.myId, str(x), msg, self.sockets, self.activeLinks)
        #leader is part of quorum and needs to "receive" the message

        self.receivePrepares(msg)
    
    #Acceptor
    def receivePrepares(self, msg):
        print("Receiving PREPARE", flush=True)
        receiver = str(msg['ballot'].procId)
        if(self.ballotNum < msg['ballot'] or receiver == self.myId):
            self.ballotNum = msg['ballot']
            msg = {
                'type': 'PROMISE',
                'ballot': self.ballotNum,
                'acceptBal': self.acceptBal,
                'acceptVal': self.acceptVal,
                'timestamp':msg['timestamp']
            }
            if(receiver != self.myId):
                print("Sending PROMISE to " + receiver, flush=True)
                sendMessage(self.myId, receiver, msg, self.sockets, self.activeLinks)
            else:

                self.receivePromise(msg)
    
    #PHASE II: Accept
    
    #Proposer
    def receivePromise(self, msg = None, proposalVal = None, timestamp = None):
        #print("Receiving Promise", flush=True)
        if proposalVal is None:
            self.promises.append((msg['acceptBal'], msg['acceptVal']))
    
        if len(self.promises) == QUORUM_SIZE or self.isLeader:
            if self.isLeader:
                opTime = timestamp
                self.proposalVal = proposalVal
                print("Already Leader. Starting Paxos from phase II -- Broadcasting ACCEPT.", flush=True)
                self.ballotNum = Ballot(self.ballotNum.seqNum+1, self.myId, self.depth)
            else:
                opTime = msg['timestamp']
                print("Received PROMISE from majority -- Broadcasting ACCEPT", flush=True)
                maxBallot = Ballot(0,self.myId,self.depth)
                for ballot, operation in self.promises:
                    if operation is not None and ballot != Ballot(0,self.myId,self.depth) and maxBallot < ballot:
                        self.proposalVal = operation
                        maxBallot = ballot
            msg = {
                    'type': 'ACCEPT',
                    'ballot': self.ballotNum,
                    'operation': self.proposalVal,
                    'timestamp': opTime
            }
            for x in range(1, NUM_OF_SERVERS+1):
                if(str(x) != self.myId):
                    print("sending ACCEPT to server " + str(x) )
                    sendMessage(self.myId, str(x), msg, self.sockets, self.activeLinks)
                else:
                    self.receiveAccept(msg)
    
    def receiveAccept(self, msg): 
        print("Receiving Accept...", flush=True)

        if not msg['ballot'] < self.ballotNum and self.onAccept:

            self.acceptBal = msg['ballot']
            self.acceptVal = msg['operation']
            receiver = str(msg['ballot'].procId)
            msg = {
                'type': 'ACCEPTED',
                'ballot': self.ballotNum,
                'timestamp':msg['timestamp']
            }
            if(receiver != self.myId):
                print("Sending ACCEPTED to server" + receiver, flush=True)
                sendMessage(self.myId, receiver, msg, self.sockets, self.activeLinks)
            else:

                self.receiveAccepted(msg)
        else:
            print("Ballot Rejected -- my ballotNum= " + str(self.ballotNum) + "\n received ballot= " + str(msg['ballot']))


    def receiveAccepted(self, msg): #leader receives accepted from listeners
        #print("Receiving accepted", flush=True)
        self.lock.acquire()
        self.accepts += 1
        self.lock.release()
        if self.accepts == QUORUM_SIZE:
            print("Received ACCEPTED from majority -- Broadcasting DECIDE", flush=True)
            #decide v
            msg = {
                'type': 'DECIDE',
                'operation': self.proposalVal,
                'ballot': self.ballotNum,
                'timestamp':msg['timestamp'],
                'fromId':self.myId
            }
            self.isLeader = True
            for x in range(1, NUM_OF_SERVERS+1):
                if(str(x) != self.myId):
                    sendMessage(self.myId, str(x), msg, self.sockets, self.activeLinks)
                else:

                    self.receiveDecision(msg)
    
    #PHASE III: Decision
    def receiveDecision(self, msg):
        print("Received DECIDE -- making decision", flush=True)
        self.depthLock.acquire()
        newDepth = self.onDecison(msg, self.isLeader)
        self.updateDepth(newDepth)
        self.depthLock.release()
        

    def updateDepth(self, newDepth):
        self.depth = newDepth
        self.ballotNum = Ballot(self.ballotNum.seqNum,self.ballotNum.procId,self.depth)
        self.acceptBal = Ballot(self.ballotNum.seqNum,self.ballotNum.procId,self.depth)
        self.acceptVal = None        
        self.accepts = 0