class Ballot:
    def __init__(self, seqNum=0, procId=0, depth=0):
        self.procId = procId
        self.seqNum = seqNum
        self.depth = depth
    
    def __lt__(self, other):
        if self.depth == other.depth:
            if self.seqNum == other.seqNum:
                return self.procId < other.procId
            else:
                return self.seqNum < other.seqNum
        else:
            return self.depth < other.depth
    
    def __eq__(self, other):
        return self.depth == other.depth and self.seqNum == other.seqNum and self.procId == other.procId

    def __str__(self):
        return '< ' + str(self.seqNum) + ', ' + str(self.procId) + ', ' + str(self.depth) + ' >'

    def __repr__(self):
        return '< ' + str(self.seqNum) + ', ' + str(self.procId) + ', ' + str(self.depth) + ' >'