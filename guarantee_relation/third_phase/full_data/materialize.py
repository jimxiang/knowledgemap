# /usr/bin/env/ python
# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from datetime import datetime
import ConfigParser
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

env = 'dev'
conf_path = '../config/' + env
cfg = ConfigParser.SafeConfigParser()
cfg.read(conf_path + '/guarantee.cfg')

reload(sys)
sys.setdefaultencoding('utf-8')


def materialize(t_guarantee, t_base_guarantee, t_custid_eid, t_base_custid_eid):
    new_batch = datetime.now().strftime('%Y%m%d')

    latest_batch = str(spark.sql("select max(batch) batch from %s" % t_guarantee).take(1)[0]['batch'])
    spark.sql(
        '''
        select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id,
            guaranteetype, currency, guaranteeamount, sgn_dt, data_src, grnt_mapid, bor_mapid
        from {t_guarantee}
        where batch = '{latest_batch}' and grnt_cod is not null and grnt_cod != ''
            and bor_cod is not null and bor_cod != ''
        '''.format(t_guarantee=t_guarantee, latest_batch=latest_batch)
    ).createOrReplaceTempView('base_table_guarantee')

    spark.sql(
        '''
        insert overwrite table {t_base_guarantee} partition(batch='{new_batch}') select * from base_table_guarantee
        '''.format(t_base_guarantee=t_base_guarantee, new_batch=new_batch))

    spark.sql('select cvm_cust_id as cust_id, eid from {t_custid_eid}'.format(t_custid_eid=t_custid_eid))\
        .createOrReplaceTempView("base_table_custid_eid")

    spark.sql(
        '''
        insert overwrite table {t_base_custid_eid} partition(batch='{new_batch}') select * from base_table_custid_eid
        '''.format(t_base_custid_eid=t_base_custid_eid, new_batch=new_batch))

    logging.info('materialize finished')


def main():
    t_guarantee = cfg.get('hivetable', 'guarantee_relation_table')  # guarantee relationship table
    t_base_guarantee = cfg.get('hivetable', 'guarantee_relation_base_table')  # materialize guarantee relationship table
    t_custid_eid = cfg.get('hivetable', 'custid_eid_table')  # customer id maps eid table
    t_base_custid_eid = cfg.get('hivetable', 'custid_eid_base_table')  # materialize customer id maps eid table

    materialize(t_guarantee, t_base_guarantee, t_custid_eid, t_base_custid_eid)


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('guarantee_relation') \
        .config('spark.log.level', 'WARN') \
        .enableHiveSupport() \
        .getOrCreate()

    main()
