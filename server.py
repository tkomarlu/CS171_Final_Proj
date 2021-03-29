from network import sendMessage, failLink, updateActiveLink, fixLink
from paxos import Paxos
from blockchain import Blockchain
from block import Block, createBlock
import socket
import threading
import pickle
import json
import sys
import os
import queue
import time

q = queue.Queue()

keyValueStore = {}
hasDecision = {}

blockchain = Blockchain()

#5 different servers.
with open("./config.json") as f:
    PORTS = json.load(f)

SOCKETS = {}
MY_ID = None
paxos = None
proposedBlock = None
network = None
queueTimeouts = {}
leaderEstimate = None
blockLock = threading.Lock()

def saveBlockchain():
    global MY_ID
    try:
        with open("blockchain" + MY_ID + ".pickle", "wb") as f:
            pickle.dump(blockchain, f, protocol=pickle.HIGHEST_PROTOCOL)
    except:
        pass

def reconnect(OTHER_IDS, CLIENTS):
#connect to all other servers. Save sockets in SERVER_SOCKETS
    print("trying to reconnect to all servers", flush=True)
    for id in OTHER_IDS:
        otherPort = PORTS[id]
        otherSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        otherSocket.connect((socket.gethostname(), int(otherPort)))
        SOCKETS[id] = otherSocket

    for client in CLIENTS:
        clientPort = PORTS[client]
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((socket.gethostname(), int(clientPort)))
        SOCKETS[client] = clientSocket

    paxos.sockets = SOCKETS
          

def connectToAll(OTHER_IDS, CLIENTS):
#connect to all other servers. Save sockets in SERVER_SOCKETS
    print("trying to connect to all", flush=True)
    for id in OTHER_IDS:
        otherPort = PORTS[id]
        otherSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        otherSocket.connect((socket.gethostname(), int(otherPort)))
        SOCKETS[id] = otherSocket
        
    for client in CLIENTS:
        clientPort = PORTS[client]
        clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        clientSocket.connect((socket.gethostname(), int(clientPort)))
        SOCKETS[client] = clientSocket
    initializePaxos()
    print("connected to all!", flush=True)


def onDecision(msg, isLeader):
    global leaderEstimate
    global q, proposedBlock, blockchain
    global blockLock
    #print("onDecision insert to blockchain", flush=True)
    blockLock.acquire()
    leaderEstimate = msg['fromId']
    currBlock = blockchain.head
    duplicateBlock = False

    while currBlock is not None:
        if currBlock == msg['operation']:
            duplicateBlock = True
        currBlock = currBlock.prev
    
    clientResponse = "Block already exists."

    hasDecision[msg['timestamp']] = True

    if duplicateBlock == False:
        blockchain.insert(msg['operation'])
        clientResponse = applyOperations(msg['operation'])
        print("Updated Blockchain.", flush=True)
    if proposedBlock:
        if isLeader and proposedBlock == msg['operation']:
            proposedBlock = None
            head = q.get()
            sendMessage(MY_ID, head["client"], {
                'msg':clientResponse,
                'operation': msg['operation'].operation,
                'type':'SERVER_RESPONSE',
                'timestamp' : msg['timestamp'],
                'fromId': MY_ID
                }, SOCKETS, paxos.activeLinks)
    blockLock.release()
    return blockchain.depth

def applyOperations(block):
    global keyValueStore
    print("Applying " + block.operation["op"] + " operation...")
    if block.operation["op"] == 'put':
        if block.operation["value"][0] == '{':
            if block.operation["key"] in keyValueStore and type(keyValueStore[block.operation["key"]]) is dict:
                keyValueStore[block.operation["key"]][block.operation["value"].split("'")[1]] = block.operation["value"].split("'")[3]
            else:
                keyValueStore[block.operation["key"]] = {}
                keyValueStore[block.operation["key"]][block.operation["value"].split("'")[1]] = block.operation["value"].split("'")[3]
        else:
            keyValueStore[block.operation["key"]] = block.operation["value"]
        return 'ACK'
    if block.operation["op"] == 'get':
        if block.operation["key"] in keyValueStore:
            return 'GET ' + str(keyValueStore[block.operation["key"]])
        else:
            return 'NO_KEY'

def onAccept(msg):
    block = msg['operation']
    return block.previousHash == blockchain.head.hash and int(block.hash[-1],16) < 4

def initializePaxos():
    global paxos
    print("Initializing Paxos", flush=True)
    paxos = Paxos(SOCKETS, MY_ID, onDecision, onAccept)
    threading.Thread(target=queueWatcher).start()

def queueWatcher():
    global proposedBlock
    global blockLock
    while True:
        blockLock.acquire()
        if not q.empty():
            ts = q.queue[0]["timestamp"]
            if ts in hasDecision:
                if hasDecision[ts] == True:
                    print("decision already made for this operation")
                    q.get()
                    continue
        if proposedBlock is None and not q.empty(): #we are ready to start another round of paxos
            if queueTimeouts[q.queue[0]["timestamp"]] == True:
                q.get()
                continue
            proposedBlock = createBlock(q.queue[0]["operation"], blockchain.head)
            if not paxos.isLeader:
                    paxos.prepareProposal(proposedBlock, q.queue[0]["timestamp"]) #start paxos
                    print("Created a proposal with a " + q.queue[0]["operation"]["op"] + " operation and ballot number of " + str(paxos.ballotNum), flush=True)
            else:
                    paxos.receivePromise(proposalVal=proposedBlock, timestamp=q.queue[0]["timestamp"])
                    print("I'm leader so sending promise with a " + q.queue[0]["operation"]["op"] + " operation and ballot number of " + str(paxos.ballotNum), flush=True)
        blockLock.release()

def printQueue():
    print("---Current Operations in queue---", flush=True)
    if q.empty():
        print("Queue is empty", flush=True)
    else:      
        for op in q.queue:
            print("Operation: ", op["operation"], flush=True)
            print("Client: ", op["client"], flush=True)
            print("Timestamp: ", op["timestamp"], flush=True)
            print("   ---------   ", flush=True)

def printBlockchain():
    global blockchain
    print(repr(blockchain), flush=True)

def printKeyValueStore():
    print(keyValueStore, flush=True)
    
def reconstructKeyVal():
    global blockchain
    global keyValueStore
    curBlock = blockchain.head
    while curBlock is not None:
        if curBlock.operation["op"] == 'put':
            if curBlock.operation["value"][0] == '{':
                if curBlock.operation["key"] in keyValueStore and type(keyValueStore[curBlock.operation["key"]]) is dict:
                    keyValueStore[curBlock.operation["key"]][curBlock.operation["value"].split("'")[1]] = curBlock.operation["value"].split("'")[3]
                else:
                    keyValueStore[curBlock.operation["key"]] = {}
                    keyValueStore[curBlock.operation["key"]][curBlock.operation["value"].split("'")[1]] = curBlock.operation["value"].split("'")[3]
            else:
                keyValueStore[curBlock.operation["key"]] = curBlock.operation["value"]
        curBlock = curBlock.prev
    return

def waitForMyTimeout(timestamp):
    global queueTimeouts
    global hasDecision
    hasDecision[timestamp] = False
    start = int(time.time())
    now = start
    while (now - start < 38):
        now = int(time.time())
        continue
    queueTimeouts[timestamp] = True

def waitForLeaderTimeout(timestamp, msg):
    global leaderEstimate
    global queueTimeouts
    global hasDecision
    hasDecision[timestamp] = False
    start = int(time.time())
    now = start
    while (now - start < 38):
        now = int(time.time())
        if hasDecision[timestamp] == True:
            return

    prevLeaderEstimate = leaderEstimate
    while leaderEstimate == prevLeaderEstimate:
        leaderEstimate = str((int(leaderEstimate)) % 5 + 1)

    if hasDecision[timestamp] == False:
        queueTimeouts[msg["timestamp"]] = False
        q.put({
            'operation' : msg["operation"],
            'timestamp' : msg["timestamp"],
            'client' : msg["client"]
        })
        threading.Thread(target=waitForMyTimeout, args=[msg["timestamp"]]).start()

def listenForMessages(senderSocket, address):
    global blockchain
    global leaderEstimate
    while True:
        try:
            message = senderSocket.recv(8192)
            if message:
                decodedMessage = pickle.loads(message)
                #print("decodedMessage type: " + decodedMessage["type"], flush=True)

                if decodedMessage["type"] == 'FAILLINK':
                    updateActiveLink(decodedMessage["src"], paxos.activeLinks, False)

                if decodedMessage["type"] == 'FIXLINK':
                    updateActiveLink(decodedMessage["src"], paxos.activeLinks, True)

                if decodedMessage["type"] == 'OPERATION':
                    if leaderEstimate == MY_ID:
                        print("adding operation to queue", flush=True)
                        queueTimeouts[decodedMessage["timestamp"]] = False
                        q.put({
                            'operation' : decodedMessage["operation"],
                            'timestamp' : decodedMessage["timestamp"],
                            'client' : decodedMessage["client"]
                        })
                        threading.Thread(target=waitForMyTimeout, args=[decodedMessage["timestamp"]]).start()
                    else:
                        sendMessage(MY_ID, leaderEstimate, decodedMessage, SOCKETS, paxos.activeLinks)
                        threading.Thread(target=waitForLeaderTimeout, args=[decodedMessage["timestamp"], decodedMessage]).start()

                if decodedMessage["type"] == 'PREPARE':
                    paxos.receivePrepares(decodedMessage)
                if decodedMessage["type"] == 'PROMISE':
                    paxos.receivePromise(decodedMessage)
                if decodedMessage["type"] == 'ACCEPT':
                    paxos.receiveAccept(decodedMessage)
                if decodedMessage["type"] == 'ACCEPTED':
                    paxos.receiveAccepted(decodedMessage)
                if decodedMessage["type"] == 'DECIDE':
                    paxos.receiveDecision(decodedMessage)

                saveBlockchain()                           
        #EOFError gets triggered somwhere here when another server is killed.
        except socket.error as e:
            print(f'Socket at {address} forcibly disconnected with {e}.')
            senderSocket.close()
            break

    return

def handleInput(OTHER_IDS, CLIENTS, MY_ID):
    global blockchain
    global SOCKETS
    while True:
        try:
            cliInput = input()
            if cliInput:
                if cliInput.lower() == "operation":
                    print("doing operation", flush=True)
                
                if cliInput.lower() == "connect":
                    print("connecting...", flush=True)
                    connectToAll(OTHER_IDS, CLIENTS)

                if cliInput.lower() == "reconnect":
                    print("reconnecting...", flush=True)
                    reconnect(OTHER_IDS, CLIENTS)
                
                if cliInput.lower() == "load blockchain":
                    with open("blockchain" + MY_ID + ".pickle", "rb") as f:
                        blockchain = pickle.load(f)
                    reconstructKeyVal()
                    #reapply operation stuff using blockchain
                        
                if cliInput.lower().split(' ')[0] == "faillink":
                    dest = cliInput.lower().split(' ')[1]
                    failLink(MY_ID, dest, SOCKETS, paxos.activeLinks)
                    
                if cliInput.lower().split(' ')[0] == "fixlink":
                    dest = cliInput.lower().split(' ')[1]
                    fixLink(MY_ID, dest, SOCKETS, paxos.activeLinks)
                                        
                if cliInput.lower() == "printqueue":
                    printQueue()

                if cliInput.lower() == "printchain":
                    printBlockchain()
                
                if cliInput.lower() == "printstore":
                    printKeyValueStore()

                if cliInput.lower() == "failprocess":
                    os._exit(0)

        except EOFError:
            print("exception")
            pass
            #do nothing
    return

def main():
    #python3 server.py my_id
    global leaderEstimate
    global MY_ID    
    if len(sys.argv) != 2:
        print("wrong number of arguments", flush=True)
    MY_ID = str(sys.argv[1])
    MY_PORT = PORTS[MY_ID]
    MY_SOCKET = socket.socket()
    MY_SOCKET.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    MY_SOCKET.bind((socket.gethostname(), int(MY_PORT)))
    MY_SOCKET.listen(32)
    print('Server' + str(MY_ID) + ' started.', flush=True)
    print('Server listening on port ' + str(MY_PORT) + '.', flush=True)
    OTHER_IDS = ["1", "2", "3", "4", "5"]
    OTHER_IDS.pop(int(MY_ID) - 1)
    leaderEstimate = "1"

    CLIENTS = ["C1", "C2", "C3"]

    threading.Thread(target=handleInput, args=(
        OTHER_IDS, CLIENTS, MY_ID)).start()

    while True:
        try:
            senderSocket, address = MY_SOCKET.accept()
            threading.Thread(target=listenForMessages, args=(senderSocket, address)).start()
        except KeyboardInterrupt:
            os._exit(0)
    return

if __name__ == "__main__":
    main()