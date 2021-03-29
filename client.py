import socket
import threading
import pickle
import json
import sys
import os
import time

with open("./config.json") as f:
    PORTS = json.load(f)
    
SERVER_SOCKETS = {}
MY_ID = 0
leaderEstimate = "1"
hasResponse = {}

def connectToServers():
    for i in range(1, 6):
        serverPort = PORTS[str(i)]
        serverSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        serverSocket.connect((socket.gethostname(), int(serverPort)))
        SERVER_SOCKETS[str(i)] = serverSocket
        
    print("connected to servers!")
    return

def broadcastToServers(message):
    
    for server in SERVER_SOCKETS:
        serverSocket = SERVER_SOCKETS[server]
        pickledMessage = pickle.dumps(message)
        serverSocket.send(pickledMessage)
    
    return

def sendToServer(message, serverId):
    serverSocket = SERVER_SOCKETS[serverId]
    pickledMessage = pickle.dumps(message)
    serverSocket.send(pickledMessage)
    return

def sendOperation(operation, currTime):
    global hasResponse
    hasResponse[currTime] = False
    threading.Thread(target=sendToServer, args=[operation, leaderEstimate]).start()
    threading.Thread(target=waitForTimeout, args=[50, operation, currTime]).start()

def waitForTimeout(timeoutAmount, operation, timestamp):
    global leaderEstimate
    global hasResponse
    start = int(time.time())
    now = start
    while (now - start < timeoutAmount):
        now = int(time.time())
        if hasResponse[timestamp] == True:
            del hasResponse[timestamp]
            break
    if now-start >= timeoutAmount:
        prevLeaderEstimate = leaderEstimate
        while leaderEstimate == prevLeaderEstimate:
            leaderEstimate = str((int(leaderEstimate)) % 5 + 1)
        print("Server " + prevLeaderEstimate + " has timed out. Trying server " + leaderEstimate, flush=True)
        sendOperation(operation, timestamp)


def handleInput():
    global MY_ID
    while True:
        try:
            cliInput = input()
            if cliInput:     
                splitInput = cliInput.lower().split(' ')
                #Input format: operation put key {'valKey': 'value'}
                if splitInput[0] == "operation":
                    currTime = str(time.time())
                    if splitInput[1] == 'get':
                        msg = {
                            'timestamp' : currTime,
                            'operation' : {
                                'op' : splitInput[1],
                                'key' : splitInput[2]
                            },
                            'type' : 'OPERATION',
                            'client' : MY_ID
                        }
                    else:
                        msg = {
                            'timestamp' : currTime,
                            'operation' : {
                                'op' : splitInput[1],
                                'key' : splitInput[2],
                                'value' : splitInput[3]
                            },
                            'type' : 'OPERATION',
                            'client' : MY_ID
                        }
                    sendOperation(msg, currTime)
                    print("Performing operation...")

                if splitInput[0] == "connect":
                    print("connecting...")
                    connectToServers()
                    
                if splitInput[0] == "send":
                    splitInput = cliInput.split()
                    message = { 
                        "msg":splitInput[3],
                        "fromId": MY_ID
                    }
                    if splitInput[1] == "server":
                        print("Sending : \"" + splitInput[3] + "\" to server " + splitInput[2])
                        sendToServer(message, splitInput[2]) #message, serverId
                        
                if splitInput[0] == "broadcast":
                    broadcastMessage = { 
                        "msg":cliInput.split()[1],
                        "fromId": MY_ID
                    }
                    print("Broadcasting: " + broadcastMessage["msg"])
                    broadcastToServers(broadcastMessage) 
        
        except EOFError:
            print("exception")
            pass
            #do nothing
    return

def listenForMessages(senderSocket, address):
    while True:
        try:
            message = senderSocket.recv(1024)
            if message:
                decodedMessage = pickle.loads(message)
                if decodedMessage:
                    if decodedMessage["type"] == 'SERVER_RESPONSE':
                        if decodedMessage["timestamp"] in hasResponse:
                            if hasResponse[decodedMessage["timestamp"]] == True:
                                continue
                        hasResponse[decodedMessage["timestamp"]] = True
                        if decodedMessage["msg"] == "ACK" or decodedMessage["msg"][0:3] == "GET":
                            leaderEstimate = decodedMessage["fromId"]
                    print("Received a message from process " + decodedMessage["fromId"] + ": " + decodedMessage["msg"])
        
        except socket.error as e:
            print(f'Socket at {address} forcibly disconnected with {e}.')
            senderSocket.close()
            break

    return

def main():
    #python3 server.py my_id
    #client ids: C1/C2/C3
    global MY_ID
    if len(sys.argv) != 2:
        print("wrong number of arguments")
        
    MY_ID = sys.argv[1]
    MY_PORT = PORTS[MY_ID]
    MY_SOCKET = socket.socket()
    MY_SOCKET.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    MY_SOCKET.bind((socket.gethostname(), int(MY_PORT)))
    MY_SOCKET.listen(32)
    print(f'Client {MY_ID} started.')
    print(f'Server listening on port {MY_PORT}.')


    threading.Thread(target=handleInput, args=()).start()

    while True:
        try:
            senderSocket, address = MY_SOCKET.accept()
            threading.Thread(target=listenForMessages, args=(senderSocket, address)).start()
        except KeyboardInterrupt:
            os._exit(0)
    return

if __name__ == "__main__":
    main()