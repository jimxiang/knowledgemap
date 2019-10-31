import os
import threading
from datetime import datetime


def get_now_date():
    return datetime.now().strftime('%Y%m%d')


class SparkConf(object):
    _instance_lock = threading.Lock()

    def __init__(self):
        self.__hdfs_nodes_file_path = ''
        self.__hdfs_graph_file_path = ''
        self.__hdfs_grnt_rel = ''
        self.__hdfs_grnt_eid_mapping = ''

        self.__t_guarantee = ''
        self.__t_base_guarantee = ''
        self.__t_custid_eid = ''
        self.__t_base_custid_eid = ''
        self.__t_exchange_rate = ''
        self.__check_point_dir = ''
        self.__graphframes_path = ''

        self.__output_rel_table = ''
        self.__output_eid_mapping_table = 'g'
        self.up_limit = 1000000
        self.low_limit = 300
        self.max_length = 1000

    def __new__(cls, *args, **kwargs):
        if not hasattr(SparkConf, '_instance'):
            with SparkConf._instance_lock:
                if not hasattr(SparkConf, '_instance'):
                    SparkConf._instance = object.__new__(cls)
        return SparkConf._instance

    def get_hdfs_nodes_file_path(self):
        return self.__hdfs_nodes_file_path

    def get_hdfs_graph_file_path(self):
        return self.__hdfs_graph_file_path

    def get_hdfs_rel_json_path(self):
        return self.__hdfs_grnt_rel

    def get_hdfs_eid_mapping_json_path(self):
        return self.__hdfs_grnt_eid_mapping

    def get_guarantee_table(self):
        return self.__t_guarantee

    def get_base_guarantee_table(self):
        return self.__t_base_guarantee

    def get_custid_eid_table(self):
        return self.__t_custid_eid

    def get_base_custid_eid_table(self):
        return self.__t_base_custid_eid

    def get_exchange_table(self):
        return self.__t_exchange_rate

    def get_check_point_path(self):
        return self.__check_point_dir

    def get_graphframes_path(self):
        return self.__graphframes_path

    def get_output_rel_table(self):
        return self.__output_rel_table

    def get_output_eid_mapping_table(self):
        return self.__output_eid_mapping_table


class FilePathConf(object):
    _instance_lock = threading.Lock()

    def __init__(self):
        # file_path = os.path.dirname(os.path.realpath(__file__))
        file_path = '/data/disk3/danbaoguanxi'
        input_nodes_filename = 'input_grnt_graph_nodes_%s' % get_now_date()
        sub_folder = '/data/'
        input_graph_filename = 'input_grnt_graph_%s' % get_now_date()
        output_rel_filename = 'output_grnt_rel_%s' % get_now_date()
        eid_mapping_filename = 'output_grnt_eid_mapping_%s' % get_now_date()

        self.__sub_dir_path = file_path + sub_folder

        self.__input_nodes_file_path = file_path + sub_folder + input_nodes_filename

        self.__input_graph_path = file_path + sub_folder + input_graph_filename

        self.__rel_path = file_path + sub_folder + output_rel_filename

        self.__eid_mapping_path = file_path + sub_folder + eid_mapping_filename

    def __new__(cls, *args, **kwargs):
        if not hasattr(FilePathConf, '_instance'):
            with FilePathConf._instance_lock:
                if not hasattr(FilePathConf, '_instance'):
                    FilePathConf._instance = object.__new__(cls)
        return FilePathConf._instance

    def get_sub_dir_path(self):
        return self.__sub_dir_path

    def get_input_nodes_file_path(self):
        return self.__input_nodes_file_path

    def get_input_graph_path(self):
        return self.__input_graph_path

    def get_output_rel_path(self):
        return self.__rel_path

    def get_output_eid_mapping_path(self):
        return self.__eid_mapping_path


BASE_SPARK = SparkConf()
BASE_FILE_PATH = FilePathConf()
