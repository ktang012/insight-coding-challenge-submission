import math
from datetime import datetime

class TransactionGraph():
    def __init__(self, window_sz=60):
        self.graph = {}
        self.running_median = 0.0
        self.window_sz = window_sz
        self.current_time = None
        
    def add_node(self, node_name):
        self.graph[node_name] = {"edges": set(),
                                 "timestamps": {}}

    def add_edge(self, node_a, node_b):
        if node_a not in self.graph:
            self.add_node(node_a)
        if node_b not in self.graph:
            self.add_node(node_b)
            
        self.graph[node_a]["edges"].add(node_b)
        self.graph[node_b]["edges"].add(node_a)
                    
    def remove_edge(self, node_a, node_b):
        try:
            self.graph[node_a]["edges"].remove(node_b)
            self.graph[node_b]["edges"].remove(node_a)
        except Exception as e:
            print("ERROR", e, "cannot remove", node_a, node_b)
            
    def add_timestamp(self, node_a, node_b, timestamp):
        self.graph[node_a]["timestamps"][node_b] = timestamp
        self.graph[node_b]["timestamps"][node_a] = timestamp
    
    def remove_outdated(self):
        for src_node in self.graph:
            for targ_node, timestamp in self.graph[src_node]['timestamps'].items():
                if timestamp == 0:
                    continue
                elif (self.current_time - timestamp).total_seconds() > self.window_sz:
                    self.remove_edge(src_node, targ_node)
                    self.graph[src_node]['timestamps'][targ_node] = 0
                    self.graph[targ_node]['timestamps'][src_node] = 0
  
    def add_transaction(self, transaction):
        target = transaction["target"]
        actor = transaction["actor"]
        timestamp = datetime.strptime(transaction["created_time"], '%Y-%m-%dT%H:%M:%SZ')
        if self.current_time == None:
            self.current_time = timestamp
            self.add_edge(target, actor)
            self.add_timestamp(target, actor, timestamp)
        else:
            time_diff = (self.current_time - timestamp).total_seconds()
            if timestamp > self.current_time:
                self.current_time = timestamp
                self.add_edge(target, actor)
                self.add_timestamp(target, actor, timestamp)
                self.remove_outdated()
            elif time_diff < self.window_sz:
                self.add_edge(target, actor)
                self.add_timestamp(target, actor, timestamp)
    
    def get_median(self, transaction):
        self.add_transaction(transaction)
        indegrees = []
        for node in self.graph:
            if len(self.graph[node]["edges"]) != 0:
                indegrees.append(len(self.graph[node]["edges"]))
        indegrees.sort() # bottleneck...
        index = int(len(indegrees) / 2)
        if len(indegrees) % 2 == 0:
            median = float(indegrees[index - 1] + indegrees[index]) / 2.0
        else:
            median = indegrees[index]
        return median
    
