# mapreduce.py

class Map(object):

    def __init__(self):
        self.table = {}

    def map(self, k, v):
        pass

    def emit(self, k, v):
        if k in self.table:
            self.table[k].append(v)
        else:
            self.table[k] = [v]

    def get_table(self):
        return self.table


class Reduce(object):

    def __init__(self):
        self.result_list = {}

    def reduce(self, k, vlist):
        pass

    def emit(self, v):
        pass
        #self.result_list.append(v)

    def get_result_list(self):
        return self.result_list







