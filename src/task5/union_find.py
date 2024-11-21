class UnionFind:
    def __init__(self, size):
        self.parent = [-1] * size

    def find(self, i):
        if self.parent[i] < 0:
            return i
        self.parent[i] = self.find(self.parent[i])
        return self.parent[i]

    def union(self, root1, root2):
        r1, r2 = self.find(root1), self.find(root2)
        if r1 != r2:
            if self.parent[r1] < self.parent[r2]:
                self.parent[r2] = r1
            elif self.parent[r1] > self.parent[r2]:
                self.parent[r1] = r2
            else:
                self.parent[r1] -= 1
                self.parent[r2] = r1