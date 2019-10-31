# /usr/bin/env/ python
# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from config import BASE_SPARK
import traceback


if __name__ == '__main__':
    try:
        spark = SparkSession \
            .builder \
            .appName('guarantee_relationship') \
            .config('spark.log.level', 'WARN') \
            .enableHiveSupport() \
            .getOrCreate()

        hdfs_rel_json = BASE_SPARK.get_hdfs_rel_json_path()

        temp = os.popen('hdfs dfs -stat %s' % hdfs_rel_json).readlines()
        if len(temp):
            json_data = spark.read.json('hdfs://%s' % hdfs_rel_json)
            json_data.createOrReplaceTempView('output_table')

            spark.table('output_table').write.mode('overwrite').saveAsTable(BASE_SPARK.get_output_rel_table())
            os.system('hive -e "INSERT OVERWRITE TABLE test.grnt1 SELECT id, value FROM {0};"'
                      .format(BASE_SPARK.get_output_rel_table()))

        hdfs_eid_mapping_json = BASE_SPARK.get_hdfs_eid_mapping_json_path()

        temp = os.popen('hdfs dfs -stat %s' % hdfs_eid_mapping_json).readlines()
        if len(temp):
            json_data = spark.read.json('hdfs://%s' % hdfs_eid_mapping_json)
            json_data.createOrReplaceTempView('eid_mapping')

            spark.table('eid_mapping').write.mode('overwrite').saveAsTable(BASE_SPARK.get_output_eid_mapping_table())
            os.system('hive -e "INSERT OVERWRITE TABLE test.grnt1_eid SELECT eid, union_id FROM {0};"'
                      .format(BASE_SPARK.get_output_eid_mapping_table()))

    except():
        e = traceback.format_exc()
        print(e)
