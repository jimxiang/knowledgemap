import threading
import json
from pandas import Series
from config import BASE_SPARK, BASE_FILE_PATH


class Nodes(object):
    _instance_lock = threading.Lock()

    def __init__(self):
        self.hdfs_node_file_path = BASE_SPARK.get_hdfs_nodes_file_path()
        self.input_nodes_file_path = BASE_FILE_PATH.get_input_nodes_file_path()
        self.nodes = self.prepare_nodes()

    def __new__(cls, *args, **kwargs):
        if not hasattr(Nodes, '_instance'):
            with Nodes._instance_lock:
                if not hasattr(Nodes, '_instance'):
                    Nodes._instance = object.__new__(cls)
        return Nodes._instance

    def prepare_nodes(self):
        try:
            nodes = {}
            with open(self.input_nodes_file_path, 'r') as f:
                for line in f.readlines():
                    line_json = json.loads(line.strip())
                    nodes[line_json['node_id']] = line_json['node']
                pd_nodes = Series(nodes)
                return pd_nodes
        except IOError:
            print('get nodes failed')
            return []

    def get_node(self, node_id):
        if self.nodes[node_id]:
            return self.nodes[node_id]
        else:
            return False

    def get_node_eid(self, node_id):
        if self.nodes[node_id]:
            node = self.nodes[node_id]
            if 'eid' in node:
                return node['eid']
            else:
                return False
        return False


NODE = Nodes()
