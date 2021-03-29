import hashlib
import pickle

class Blockchain(object):
    def __init__(self, head=None):
        self._head = head
        self._depth = 0
        
    @property
    def head(self):
        return self._head
    
    @head.setter
    def head(self, newHead):
        self._head = newHead
        
    @property
    def depth(self):
        return self._depth

    @depth.setter
    def depth(self, val):
        self._depth = val
    
    def insert(self, block):
        block.prev = self.head
        self.head = block
        self.depth = self.depth + 1
    
    def __repr__(self):
        block = self.head
        blocks = []
        while block is not None:
            blocks.append(str(block))
            block = block.prev
        blocks.append("NULL")
        blocks.reverse()
        return "\n ↑ \n".join(blocks)