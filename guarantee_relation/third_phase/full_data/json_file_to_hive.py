# /usr/bin/env/ python
# -*- coding: utf-8 -*-
import os
from pyspark.sql import SparkSession
from config import BASE_SPARK


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('guarantee_relation') \
        .config('spark.log.level', 'WARN') \
        .enableHiveSupport() \
        .getOrCreate()

    hdfs_list_json = BASE_SPARK.get_hdfs_list_json_path()

    temp = os.popen('hdfs dfs -stat %s' % hdfs_list_json).readlines()
    if len(temp):
        json_data = spark.read.json('hdfs://%s' % hdfs_list_json)
        json_data.createOrReplaceTempView('output_table')

        spark.table('output_table').write.mode('overwrite').saveAsTable(BASE_SPARK.get_output_list_table())
        os.system('hive -e "INSERT OVERWRITE TABLE {0} SELECT id, value FROM {1};"'
                  .format(BASE_SPARK.get_hbase_guarantee_list_table(), BASE_SPARK.get_output_list_table()))
