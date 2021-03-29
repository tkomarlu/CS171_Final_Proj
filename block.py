import hashlib
import pickle
import time
from prettytable import PrettyTable

class Block(object):
    def __init__(self):
        self._nonce = None
        self._prevHashVal = None
        self._operation = None
        self._prev = None
    
    @property
    def hash(self):
        msg = {
            "Operation":self.operation,
            "Nonce":self.nonce,
            "PrevHashVal":self._prevHashVal
        }
        return hashlib.sha256(pickle.dumps(msg)).hexdigest()
    
    def randomizeNonce(self):
        self._nonce = hex(0)
        msg = {
            "Operation":self.operation,
            "Nonce":self.nonce
        }
        while int(hashlib.sha256(pickle.dumps(msg)).hexdigest()[-1],16) > 2:
            self._nonce = hex(int(self._nonce, 16) + 1)
            msg = {
                "Operation":self.operation,
                "Nonce":self.nonce
            }
        print("Created Nonce = " + str(self._nonce) + "\nThe hash of the block is = " + hashlib.sha256(pickle.dumps(msg)).hexdigest())
        return
    
    @property
    def nonce(self):
        return self._nonce
    
    @property
    def prevHashVal(self):
        return self._prevHashVal
    
    @prevHashVal.setter
    def prevHashVal(self, newHashVal):
        self._prevHashVal = newHashVal

    @property
    def operation(self):
        return self._operation

    @operation.setter
    def operation(self, op):
        self._operation = op

    @property
    def prev(self):
        return self._prev

    @prev.setter
    def prev(self, newPrev):
        self._prev = newPrev

    def __repr__(self):
        table = PrettyTable()
        table.align = "c"
        table.add_column("Operation",[self.operation])
        table.add_column("Nonce", [self.nonce])
        table.add_column("Prev Hash Value", [self.prevHashVal])
        table._max_width = {"Operation" : 30, "Nonce" : 10, "Prev Hash Value": 10}
        return table.get_string()

    def __eq__(self, other):
        return self.operation == other.operation and self.prev == other.prev and self.prevHashVal == other.prevHashVal

def createBlock(operation, prevBlock = None):
        b = Block()
        b.operation = operation
        b.prev = prevBlock
        b.prevHashVal = prevBlock.hash if prevBlock is not None else '0'*256
        print("Created previous hash pointer: " + b.prevHashVal)
        b.randomizeNonce()
        return b