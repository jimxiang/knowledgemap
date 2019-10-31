#!/usr/bin/env python
# -- coding:utf-8 --
import sys
import networkx as nx
import json
from datetime import datetime
import multiprocessing as mp
import logging
from config import BASE_SPARK, BASE_FILE_PATH
from pandas import Series
import os

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def get_date():
    return datetime.now().strftime('%Y%m%d %H:%M:%S')


class Graph(object):
    def __init__(self, edges):
        self.edges_num = len(edges)
        self.graph = self.generate_graph(edges)
        self.__instance_id = ''
        self.all_nodes = []
        self.__actual_nodes = []

    @staticmethod
    def generate_graph(edges):
        """
        Generate graph
        :param edges:
        :return:
        """
        try:
            edges = [(x['link'][0], x['link'][1], {
                'attrs': x['attrs']
            }) for x in edges]
            graph = nx.DiGraph(edges)
            return graph
        except nx.NetworkXException:
            return False

    @staticmethod
    def all_nodes_with_attrs(nodes):
        """
        Get all node with attributes
        :return:
        """
        all_nodes = []
        for node in nodes:
            node_attr = NODE.get_node(node)
            all_nodes.append(node_attr)
        return all_nodes

    @staticmethod
    def links_to_pairs(links):
        pairs_dict = {}
        for i in range(len(links)):
            temp_list = []
            for link in links[i]:
                for j in range(len(link) - 1):
                    _temp = [link[j], link[j + 1]]
                    if _temp not in temp_list:
                        temp_list.append(_temp)
            pairs_dict[links.index[i]] = temp_list
        return Series(pairs_dict)

    def get_instance_id(self):
        return self.__instance_id

    def __set_instance_id(self, ins_id):
        self.__instance_id = ins_id

    def set_actual_nodes(self, nodes):
        self.__actual_nodes = nodes

    def set_all_nodes(self):
        """
        Set all nodes in Graph instance
        :return:
        """
        try:
            all_nodes = [n for n in self.graph.nodes()]
            self.all_nodes = all_nodes
            ins_id = ''.join(['[graph_', str(self.edges_num), '_', str(len(all_nodes)), ']'])
            self.__set_instance_id(ins_id)
        except nx.NetworkXException:
            self.all_nodes = []

    def all_links(self):
        """
        All links in graph
        :return:
        """
        try:
            graph = self.graph
            all_pairs_links = nx.all_pairs_shortest_path(graph)
            all_links = [path[1][key] for path in all_pairs_links for key in path[1] if len(path[1][key]) == 2]
            actual_nodes = self.all_nodes
            self.set_actual_nodes(actual_nodes)
            logging.info('link length: %d' % len(all_links))
            link_list = [dict({'source_id': str(link[0]),
                               'target_id': str(link[1])}.items() + graph[link[0]][link[1]].items()) for link
                         in all_links]

            _link_result = {'paths': link_list, 'type': 'link', 'circle_id': None}
            return _link_result
        except (nx.NetworkXException, nx.NetworkXNoPath):
            return []

    def get_links(self, cutoff):
        graph = self.graph
        all_links = []
        pairs_links = nx.all_pairs_shortest_path(graph, cutoff)
        for link in pairs_links:
            all_links.append(link)

        ret_links = {}
        # 1. Find the path starting from target which path's length <= cutoff
        for links in all_links:
            _id = links[0]
            _links = links[1]
            temp = []
            for k in _links:
                if len(_links[k]) > 1:
                    temp.append(_links[k])
            ret_links[_id] = temp

        # Turn to pandas for fast key-value search
        pd_ret_links = Series(ret_links)

        # 2. Find the path ending at target which path's length <= cutoff
        for links in all_links:
            _links = links[1]
            for k in _links:
                if len(_links[k]) > 1:
                    pd_ret_links[k].append(_links[k])
        return pd_ret_links

    @staticmethod
    def get_nodes(pd_ret_links):
        ret_nodes = {}
        # Find the target's related nodes
        for i in range(len(pd_ret_links)):
            node_list = []
            for links in pd_ret_links[i]:
                for node_id in links:
                    node_list.append(node_id)
            ret_nodes[pd_ret_links.index[i]] = list(set(node_list))
        # Turn to pandas for fast key-value search
        pd_ret_nodes = Series(ret_nodes)
        return pd_ret_nodes

    def cutoff_links(self, cutoff, queue):
        try:
            graph = self.graph

            pd_ret_links = self.get_links(cutoff)
            pd_ret_nodes = self.get_nodes(pd_ret_links)

            pd_ret_links_limit = self.get_links(1)
            pd_ret_nodes_limit = self.get_nodes(pd_ret_links_limit)

            # Filter path and get related cycles
            _links = self.links_to_pairs(pd_ret_links)
            _links_limit = self.links_to_pairs(pd_ret_links_limit)

            for i in range(len(_links)):
                if (len(_links[i])) > BASE_SPARK.max_length:
                    logging.info('node %s \'s links length is %d' % (pd_ret_nodes.index[i], len(_links[i])))
                    filter_links = _links_limit[i]
                    logging.info('node %s \'s cutoff 1 links length is %d' % (pd_ret_nodes.index[i], len(filter_links)))
                    actual_nodes = pd_ret_nodes_limit[i]
                else:
                    filter_links = _links[i]
                    actual_nodes = pd_ret_nodes[i]

                lk_list = [dict({'source_id': str(lk[0]),
                                 'target_id': str(lk[1])}.items() + graph[lk[0]][lk[1]].items()) for lk
                           in filter_links]
                cy_list = []

                if len(lk_list):
                    _link_result = {'paths': lk_list, 'type': 'link', 'circle_id': None}
                    cy_list.append(_link_result)

                nd_list = self.all_nodes_with_attrs(actual_nodes)
                node_id = pd_ret_nodes.index[i]
                node_eid = NODE.get_node_eid(node_id)
                if node_eid:
                    temp = {
                        "id": node_eid,
                        "value": json.dumps({"nodes": nd_list, "links": cy_list})
                    }
                    queue.put(json.dumps(temp))
        except (nx.NetworkXException, nx.NetworkXNoPath):
            return []


def producer(edges, queue, cutoff):
    """
    Queue's producer.
    Generate graph and calculate graph's basic elements includes nodes, cycles and so on.
    :param edges: Graph's edges with attributes
    :param queue: Writer queue
    :param cutoff: Large graph cutoff
    :return: Class Graph's instance
    """
    graph = Graph(edges)
    if BASE_SPARK.low_limit <= len(edges) < BASE_SPARK.up_limit:
        process_large_graph(graph, queue, cutoff)
    if len(edges) < BASE_SPARK.low_limit:
        process_small_graph(graph, queue)


def process_small_graph(graph, queue):
    try:
        logging.info('small graph')
        graph.set_all_nodes()

        all_nodes = list(set(graph.all_nodes))
        link_result = graph.all_links()
        node_result = graph.all_nodes_with_attrs(all_nodes)

        value = json.dumps({"nodes": node_result, "links": [link_result]})

        for node in all_nodes:
            eid = NODE.get_node_eid(node)
            if eid:
                queue.put(json.dumps({"id": eid, "value": value}))
    except Exception as e:
        logging.error('Process small graph %s failed: %s' % (graph.get_instance_id(), e))
        return False


def process_large_graph(graph, queue, cutoff):
    cutoff = int(cutoff) or 10
    try:
        logging.info('large graph')
        graph.cutoff_links(cutoff, queue)
    except Exception as e:
        logging.error('Process large graph %s failed: %s' % (graph.get_instance_id(), e))


def customer(queue, lock, fp):
    while True:
        data = queue.get()
        lock.acquire()
        fp.write(data + '\n')
        fp.flush()
        lock.release()


def merge_file():
    sub_dir_path = BASE_FILE_PATH.get_sub_dir_path()
    input_graph_file_path = BASE_FILE_PATH.get_input_graph_path()
    input_nodes_file_path = BASE_FILE_PATH.get_input_nodes_file_path()
    grnt_list_path = BASE_FILE_PATH.get_output_list_path()

    os.system('''
        if [ ! -d "{0}" ]; then
            mkdir {0}
        else
            rm {0}*
        fi
    '''.format(sub_dir_path))

    if not os.path.exists(input_graph_file_path):
        os.system('touch %s' % input_graph_file_path)
        os.system('hdfs dfs -getmerge %s %s' % (BASE_SPARK.get_hdfs_graph_file_path(), input_graph_file_path))

    if not os.path.exists(input_nodes_file_path):
        os.system('hdfs dfs -getmerge %s %s' % (BASE_SPARK.get_hdfs_nodes_file_path(), input_nodes_file_path))

    if not os.path.exists(grnt_list_path):
        os.system('touch %s' % grnt_list_path)

    local_graph_size = int(os.popen('ls -la {0} | cut -d " " -f 5'.format(input_graph_file_path)).readlines()[0])
    hdfs_graph_size = int(
        os.popen('hdfs dfs -du -s {0} | cut -d " " -f 1'.format(BASE_SPARK.get_hdfs_graph_file_path())).readlines()[0])

    local_nodes_size = int(os.popen('ls -la {0} | cut -d " " -f 5'.format(input_nodes_file_path)).readlines()[0])
    hdfs_nodes_size = int(
        os.popen('hdfs dfs -du -s {0} | cut -d " " -f 1'.format(BASE_SPARK.get_hdfs_nodes_file_path())).readlines()[0])

    if local_graph_size != hdfs_graph_size:
        return False

    if local_nodes_size != hdfs_nodes_size:
        return False

    return True


def main(output_path, cutoff=10):
    manager = mp.Manager()
    queue = manager.Queue()
    lock = manager.Lock()
    pool = mp.Pool(mp.cpu_count() + 2)

    fp = open(output_path, 'wb')
    writer = mp.Process(target=customer, args=(queue, lock, fp,))

    writer.start()

    jobs = []
    input_file = BASE_FILE_PATH.get_input_graph_path()
    idx = 0
    with open(input_file, 'r') as f:
        for line in f.readlines():
            idx = idx + 1
            line_json = json.loads(line.strip())
            edges = line_json['links']
            job = pool.apply_async(producer, (edges, queue, cutoff,))
            jobs.append(job)

    for job in jobs:
        job.get()

    pool.close()
    pool.join()

    while True:
        if not queue.qsize():
            writer.terminate()
            writer.join()
            break

    fp.close()


if __name__ == '__main__':
    logging.info('=====Processing start at %s!!!=====' % get_date())

    stat = merge_file()
    if not stat:
        logging.error('Get file from HDFS error!')
        sys.exit(1)

    from utils import NODE

    level = int(sys.argv[1]) or 10
    output_list_path = BASE_FILE_PATH.get_output_list_path()

    main(output_list_path, level)

    tmp = os.popen('hdfs dfs -stat %s' % BASE_SPARK.get_hdfs_list_json_path()).readlines()
    if len(tmp):
        os.system('hdfs dfs -rm %s' % BASE_SPARK.get_hdfs_list_json_path())
    os.system('hdfs dfs -put %s %s' % (output_list_path, BASE_SPARK.get_hdfs_list_json_path()))

    logging.info('=====Processing done at %s!!!=====' % get_date())
