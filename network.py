import time
import threading
import pickle

QUORUM_SIZE = 3
NUM_OF_SERVERS = 5
CLIENTS = ["C1", "C2", "C3"]
    
def sendMessage(src, dest, message, sockets, activeLinks):
    if dest in CLIENTS:
        message['sender'] = src
        threading.Thread(target=sendDelayedMessage, args=(dest, message, sockets, activeLinks)).start()
        return
    if activeLinks[dest]:
        message['sender'] = src
        threading.Thread(target=sendDelayedMessage, args=(dest, message, sockets, activeLinks)).start()

def failLink(src, dest, sockets, activeLinks):
    try:
        activeLinks[dest] = False
        socket = sockets[str(dest)]
        message = {
            'type' : 'FAILLINK',
            'src': src
        }
        pickledMessage = pickle.dumps(message)
        socket.send(pickledMessage)
    except:
        print("exception: can't send message to " + dest)
        pass

def fixLink(src, dest, sockets, activeLinks):
    try:
        activeLinks[dest] = True
        socket = sockets[str(dest)]
        message = {
            'type' : 'FIXLINK',
            'src': src
        }
        pickledMessage = pickle.dumps(message)
        socket.send(pickledMessage)
    except:
        print("exception: can't send message to " + dest)
        pass

def sendDelayedMessage(dest, message, sockets, activeLinks):
    try:
        time.sleep(3)
        socket = sockets[dest]
        pickledMessage = pickle.dumps(message)
        socket.send(pickledMessage)
    except:
        print("exception: can't send message to " + dest)
        pass

def updateActiveLink(dest, activeLinks, active):
    activeLinks[dest] = active