#!/usr/bin/env python
# -- coding:utf-8 --
import hashlib
import random
import sys
import uuid
import networkx as nx
import json
import time
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
        self.__all_cycles = []
        self.__cycle_pairs = []
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
            logging.info('===Graph %s got all nodes at %s and all nodes length is %s===' %
                         (ins_id, get_date(), len(all_nodes)))
        except nx.NetworkXException:
            self.all_nodes = []

    def set_all_cycles(self):
        """
        Set all cycles in Graph instance
        :return:
        """
        try:
            all_cycles = nx.simple_cycles(self.graph)
            cycles = [cycle for cycle in all_cycles]
            logging.info('===Graph %s got all cycles at %s and all cycles length is %s===' %
                         (self.get_instance_id(), get_date(), len(cycles)))
            self.__all_cycles = cycles
        except (nx.NetworkXException, nx.NetworkXNoCycle):
            self.__all_cycles = []

    def set_all_cycle_pairs(self):
        """
        Set all cycle pairs in Graph instance
        :return:
        """
        cycle_pairs = []
        for cycle in self.__all_cycles:
            for c in range(len(cycle)):
                t = []
                if c == len(cycle) - 1:
                    t.append(cycle[c])
                    t.append(cycle[0])
                else:
                    t.append(cycle[c])
                    t.append(cycle[c + 1])
                cycle_pairs.append(''.join(t))
        logging.info('===Graph %s got all cycle pairs at %s and cycle paris length is %s and '
                     'cycle paris distinct length is %s===' %
                     (self.get_instance_id(), get_date(), len(cycle_pairs), len(list(set(cycle_pairs)))))
        self.__cycle_pairs = list(set(cycle_pairs))

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
            cycle_pairs = self.__cycle_pairs
            links = [link for link in all_links if (''.join(link)) not in cycle_pairs]
            link_list = [dict({'source_id': str(link[0]),
                               'target_id': str(link[1])}.items() + graph[link[0]][link[1]].items()) for link
                         in links]

            _link_result = {'paths': link_list, 'type': 'link', 'circle_id': None}
            return _link_result
        except (nx.NetworkXException, nx.NetworkXNoPath):
            return []

    def cutoff_links(self, cutoff):
        """
        cutoff level of path
        :param cutoff:
        :return:
        """
        try:
            graph = self.graph
            all_links = []
            pairs_links = nx.all_pairs_shortest_path(graph, cutoff)
            for link in pairs_links:
                all_links.append(link)

            ret_links = {}
            ret_nodes = {}
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

            # 3. Find the target's related nodes
            for i in range(len(pd_ret_links)):
                node_list = []
                for links in pd_ret_links[i]:
                    for node_id in links:
                        node_list.append(node_id)
                ret_nodes[pd_ret_links.index[i]] = list(set(node_list))
            # Turn to pandas for fast key-value search
            pd_ret_nodes = Series(ret_nodes)

            # 4. Filter path and get related cycles
            results = []
            _links = self.links_to_pairs(pd_ret_links)
            logging.info('===Graph %s got cutoff links at %s and length is %s===' %
                         (self.get_instance_id(), get_date(), len(_links)))
            for i in range(len(_links)):
                lk_list = [dict({'source_id': str(lk[0]),
                                 'target_id': str(lk[1])}.items() + graph[lk[0]][lk[1]].items()) for lk
                           in _links[i]]
                actual_nodes = pd_ret_nodes[i]
                self.set_actual_nodes(actual_nodes)
                _result = self.cutoff_cycles(_links[i])
                cy_list = _result['cycle_result']
                act_nodes = list(set(_result['actual_nodes']))

                if len(lk_list):
                    _link_result = {'paths': lk_list, 'type': 'link', 'circle_id': None}
                    cy_list.append(_link_result)
                nd_list = self.all_nodes_with_attrs(act_nodes)
                node_id = pd_ret_nodes.index[i]
                node_eid = NODE.get_node_eid(node_id)
                union_id = create_union_id()
                if node_eid:
                    temp = {
                        "id": union_id,
                        "value": json.dumps({"nodes": nd_list, "links": cy_list})
                    }
                    results.append({"eid": node_eid, "union_id": union_id, "value": json.dumps(temp)})
            return results
        except (nx.NetworkXException, nx.NetworkXNoPath):
            return []

    def cutoff_cycles(self, links):
        graph = self.graph
        nodes = self.__actual_nodes
        sub_graph = nx.DiGraph(links)
        gen_cycles = nx.simple_cycles(sub_graph)
        cycles = [cycle for cycle in gen_cycles]
        _related_cycle_nodes = []

        if len(cycles):
            for cycle in cycles:
                for n in cycle:
                    if n not in nodes:
                        _related_cycle_nodes.append(n)
            cycle_list = []
            for cycle in cycles:
                temp_list = []
                if len(cycle) == 2:
                    source_id = cycle[0]
                    target_id = cycle[1]
                    if graph.has_edge(source_id, target_id):
                        temp = {'source_id': str(source_id),
                                'target_id': str(target_id)}
                        temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))
                    source_id = cycle[1]
                    target_id = cycle[0]
                    if graph.has_edge(source_id, target_id):
                        temp = {'source_id': str(source_id),
                                'target_id': str(target_id)}
                        temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))

                    cycle_list.append(temp_list)
                else:
                    for i in range(len(cycle)):
                        if i == (len(cycle) - 1):
                            source_id = cycle[i]
                            target_id = cycle[0]
                            if graph.has_edge(source_id, target_id):
                                temp = {'source_id': str(source_id),
                                        'target_id': str(target_id)}
                                temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))
                        else:
                            source_id = cycle[i]
                            target_id = cycle[i + 1]
                            if graph.has_edge(source_id, target_id):
                                temp = {'source_id': str(source_id),
                                        'target_id': str(target_id)}
                                temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))

                    cycle_list.append(temp_list)

            cycle_result = [{
                'paths': cycle,
                'type': 'circle' if len(cycle) > 2 else 'each_other' if len(cycle) == 2 else 'self',
                'circle_id': create_union_id()
            } for cycle in cycle_list]

            return {'cycle_result': cycle_result, 'actual_nodes': nodes + _related_cycle_nodes}
        else:
            return {'cycle_result': [], 'actual_nodes': nodes}

    def all_cycles(self):
        """
        All cycle in graph
        :return:
        """
        try:
            graph = self.graph
            _related_cycle_nodes = []
            actual_cycles = self.__all_cycles

            cycle_list = []
            for cycle in actual_cycles:
                temp_list = []
                if len(cycle) == 2:
                    source_id = cycle[0]
                    target_id = cycle[1]
                    if graph.has_edge(source_id, target_id):
                        temp = {'source_id': str(source_id),
                                'target_id': str(target_id)}
                        temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))
                    source_id = cycle[1]
                    target_id = cycle[0]
                    if graph.has_edge(source_id, target_id):
                        temp = {'source_id': str(source_id),
                                'target_id': str(target_id)}
                        temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))

                    cycle_list.append(temp_list)
                else:
                    for i in range(len(cycle)):
                        if i == (len(cycle) - 1):
                            source_id = cycle[i]
                            target_id = cycle[0]
                            if graph.has_edge(source_id, target_id):
                                temp = {'source_id': str(source_id),
                                        'target_id': str(target_id)}
                                temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))
                        else:
                            source_id = cycle[i]
                            target_id = cycle[i + 1]
                            if graph.has_edge(source_id, target_id):
                                temp = {'source_id': str(source_id),
                                        'target_id': str(target_id)}
                                temp_list.append(dict(temp.items() + graph[source_id][target_id].items()))

                    cycle_list.append(temp_list)

            cycle_result = [{
                'paths': cycle,
                'type': 'circle' if len(cycle) > 2 else 'each_other' if len(cycle) == 2 else 'self',
                'circle_id': create_union_id()
            } for cycle in cycle_list]

            return {'cycle_result': cycle_result, 'actual_nodes': self.__actual_nodes + _related_cycle_nodes}
        except nx.NetworkXException:
            return []


def create_union_id():
    t = ''.join([create_date(), str(uuid.uuid1())])
    m = hashlib.md5()
    m.update(bytes(str(t)))
    return m.hexdigest()


def create_date():
    a1 = (1900, 1, 1, 0, 0, 0, 0, 0, 0)
    a2 = (3000, 12, 31, 23, 59, 59, 0, 0, 0)

    start = time.mktime(a1)
    end = time.mktime(a2)

    t = random.randint(start, end)
    date_tuple = time.localtime(t)
    date = time.strftime("%Y-%m-%d %H:%m:%s", date_tuple)
    return date


def worker(queue_sg, queue_lg, edges):
    """
    Queue's producer.
    Generate graph and calculate graph's basic elements includes nodes, cycles and so on.
    Put graph instance to the queue.
    :param queue_sg: Queue instance for small graph
    :param queue_lg: Queue instance for large graph
    :param edges: Graph's edges with attributes
    :return: Class Graph's instance
    """
    graph = Graph(edges)
    if BASE_SPARK.low_limit <= len(edges) < BASE_SPARK.up_limit:
        queue_lg.put(graph)
    if len(edges) < BASE_SPARK.low_limit:
        queue_sg.put(graph)
    return graph


def listener_small_graph(queue, q_write):
    """
    Queue's consumer for complete graph calculation.
    Get graph instance for further process includes calculating links, related cycles and nodes.
    Write result to file.
    :param queue: Calculating queue
    :param q_write: Writing queue
    :return:
    """
    while True:
        graph = queue.get()
        if graph == 'end':
            break
        graph.set_all_nodes()
        graph.set_all_cycles()
        graph.set_all_cycle_pairs()

        all_nodes = list(set(graph.all_nodes))
        link_result = graph.all_links()
        cycle_result = graph.all_cycles()['cycle_result']
        node_result = graph.all_nodes_with_attrs(all_nodes)
        logging.info('Graph %s received complete result at %s and node length is %s'
                     ' and path length is %s and cycle length is %s' %
                     (graph.get_instance_id(), get_date(), len(node_result),
                      len(link_result['paths']), len(cycle_result)))

        if len(link_result['paths']):
            cycle_result.append(link_result)

        value = json.dumps({"nodes": node_result, "links": cycle_result})
        union_id = create_union_id()
        result = json.dumps({"id": union_id, "value": value})
        q_write.put(json.dumps({"value": result}))

        for node in all_nodes:
            eid = NODE.get_node_eid(node)
            if eid:
                result = json.dumps({"union_id": union_id, "eid": eid})
                q_write.put(json.dumps({"mapping": result}))
    return


def listener_large_graph(queue, q_write, cutoff):
    """
    Queue's consumer for cutoff graph calculation.
    Get graph instance for further process includes calculating links, related cycles and nodes.
    Write result to file.
    :param queue: Calculating queue
    :param q_write: Writing queue
    :param cutoff: Cutoff
    :return:
    """
    cutoff = int(cutoff) or 10
    while True:
        graph = queue.get()
        if graph == 'end':
            break
        results = graph.cutoff_links(cutoff)
        logging.info('===Graph %s received cutoff results and results length is %s===' %
                     (graph.get_instance_id(), len(results)))
        for result in results:
            eid = result['eid']
            union_id = result['union_id']
            value = result['value']
            q_write.put(json.dumps({"value": value}))
            q_write.put(json.dumps({"mapping": json.dumps({"union_id": union_id, "eid": eid})}))
    return


def customer(queue, lock, f_rel, f_eid):
    while True:
        data = queue.get()
        obj = json.loads(data)
        if 'value' in obj:
            lock.acquire()
            f_rel.write(obj['value'] + '\n')
            f_rel.flush()
            lock.release()
        if 'mapping' in obj:
            lock.acquire()
            f_eid.write(obj['mapping'] + '\n')
            f_eid.flush()
            lock.release()


def merge_file():
    sub_dir_path = BASE_FILE_PATH.get_sub_dir_path()
    input_graph_file_path = BASE_FILE_PATH.get_input_graph_path()
    input_nodes_file_path = BASE_FILE_PATH.get_input_nodes_file_path()
    grnt_rel_path = BASE_FILE_PATH.get_output_rel_path()

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

    if not os.path.exists(grnt_rel_path):
        os.system('touch %s' % grnt_rel_path)

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


def main(rel_path, eid_mapping_path, cutoff=10):
    manager = mp.Manager()
    queue_sg = manager.Queue()
    queue_lg = manager.Queue()
    queue_write = manager.Queue()
    lock = manager.Lock()
    pool = mp.Pool(mp.cpu_count() + 2)

    f_rel = open(rel_path, 'wb')
    f_eid = open(eid_mapping_path, 'wb')

    pool.apply_async(listener_small_graph, (queue_sg, queue_write,))
    pool.apply_async(listener_large_graph, (queue_lg, queue_write, cutoff,))
    writer = mp.Process(target=customer, args=(queue_write, lock, f_rel, f_eid,))

    jobs = []
    input_file = BASE_FILE_PATH.get_input_graph_path()
    with open(input_file, 'r') as f:
        for line in f.readlines():
            line_json = json.loads(line.strip())
            edges = line_json['links']
            job = pool.apply_async(worker, (queue_sg, queue_lg, edges,))
            jobs.append(job)

    for job in jobs:
        job.get()

    writer.start()

    queue_sg.put('end')
    queue_lg.put('end')
    pool.close()
    pool.join()

    while True:
        if not queue_write.qsize():
            writer.terminate()
            writer.join()
            break

    f_rel.close()
    f_eid.close()


if __name__ == '__main__':
    logging.info('=====Processing start at %s!!!=====' % get_date())

    stat = merge_file()
    if not stat:
        logging.info('Get file from HDFS error!')
        sys.exit(1)

    from utils import NODE

    level = int(sys.argv[1]) or 10
    output_rel_path = BASE_FILE_PATH.get_output_rel_path()
    output_eid_mapping_path = BASE_FILE_PATH.get_output_eid_mapping_path()

    main(output_rel_path, output_eid_mapping_path, level)

    tmp = os.popen('hdfs dfs -stat %s' % BASE_SPARK.get_hdfs_rel_json_path()).readlines()
    if len(tmp):
        os.system('hdfs dfs -rm %s' % BASE_SPARK.get_hdfs_rel_json_path())
    os.system('hdfs dfs -put %s %s' % (output_rel_path, BASE_SPARK.get_hdfs_rel_json_path()))

    tmp = os.popen('hdfs dfs -stat %s' % BASE_SPARK.get_hdfs_eid_mapping_json_path()).readlines()
    if len(tmp):
        os.system('hdfs dfs -rm %s' % BASE_SPARK.get_hdfs_eid_mapping_json_path())
    os.system('hdfs dfs -put %s %s' % (output_eid_mapping_path, BASE_SPARK.get_hdfs_eid_mapping_json_path()))

    logging.info('=====Processing done at %s!!!=====' % get_date())
