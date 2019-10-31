# /usr/bin/env/ python
# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from datetime import datetime, timedelta
from config import BASE_SPARK

sys.path.append(BASE_SPARK.get_graphframes_path())
from graphframe import GraphFrame

reload(sys)
sys.setdefaultencoding('utf-8')


def get_now_date():
    return datetime.now().strftime('%Y%m%d')


def get_date_before_today():
    today = datetime.now()
    delta = timedelta(days=-1)
    yesterday = today + delta
    return yesterday.strftime('%Y-%m-%d')


def get_batch():
    today = datetime.now()
    delta = timedelta(days=-1)
    yesterday = today + delta
    return yesterday.strftime('%Y%m%d')


class ConnectedComponents(object):
    def __init__(self, table_guarantee, base_table_guarantee, table_custid_eid, base_table_custid_eid,
                 table_exchange_rate):
        self.table_guarantee = table_guarantee
        self.base_table_guarantee = base_table_guarantee
        self.table_custid_eid = table_custid_eid
        self.base_table_custid_eid = base_table_custid_eid
        self.table_exchange_rate = table_exchange_rate
        self.today_date = get_now_date()
        self.yesterday_date = get_date_before_today()
        self.batch = get_batch()

    def materialize(self):
        table_guarantee = self.table_guarantee
        base_table_guarantee = self.base_table_guarantee
        table_custid_eid = self.table_custid_eid
        base_table_custid_eid = self.base_table_custid_eid
        today_date = self.today_date
        try:
            latest_batch = str(spark.sql("select max(batch) batch from %s" % table_guarantee).take(1)[0]['batch'])
            spark.sql(
                '''
                select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id,
                    guaranteetype, currency, guaranteeamount, sgn_dt, data_src, grnt_mapid, bor_mapid
                from %s
                where batch = '%s' and grnt_custid is not null and grnt_custid != ''
                    and bor_custid is not null and bor_custid != ''
                ''' % (table_guarantee, latest_batch)
            ).createOrReplaceTempView('base_table_guarantee')
            spark.sql(
                '''
                insert overwrite table %s partition(batch='%s') select * from base_table_guarantee
                ''' % (base_table_guarantee, today_date))

            spark.sql('select cvm_cust_id as cust_id, eid from %s' % table_custid_eid)\
                .createOrReplaceTempView("base_table_custid_eid")
            spark.sql(
                '''
                insert overwrite table %s partition(batch='%s') select * from base_table_custid_eid
                ''' % (base_table_custid_eid, today_date))

            print('===============materialize finished===================')

        except Exception as err:
            print(err)
            return False

    def connected_components(self):
        base_table_guarantee = self.base_table_guarantee
        base_table_custid_eid = self.base_table_custid_eid
        table_exchange_rate = self.table_exchange_rate
        today_date = self.today_date
        yesterday_date = self.yesterday_date

        spark.sql(
            '''
            select t1.grnt_custid as node_id, t2.eid, 
                substr(t1.grnt_custid, 0, 2) as node_type, t1.grnt_nm as node_name
            from 
            (select * from {0} where batch = '{2}') t1
            left outer join
            (select * from {1} where batch = '{2}') t2
            on t1.grnt_custid = t2.cust_id
            union
            select t3.bor_custid as node_id, t4.eid, 
                substr(t3.bor_custid, 0, 2) as node_type, t3.bor_nm as node_name
            from
            (select * from {0} where batch = '{2}') t3
            left outer join
            (select * from {1} where batch = '{2}') t4
            on t3.bor_custid = t4.cust_id
            '''.format(base_table_guarantee, base_table_custid_eid, today_date)
        ).createOrReplaceTempView('guarantee_tag')

        spark.sql(
            '''
            select a.node_id, case when a.eid is not null then a.eid when a.node_type = 'CM'
                then b.eid end as eid, a.node_type, a.node_name
            from guarantee_tag a
            left outer join (select eid, name from enterprises.hive_enterprises) b
            on a.node_name = b.name
            ''').distinct().createOrReplaceTempView("guarantee_tag_table")

        spark.sql(
            '''
            select t1.*, case when t2.cnv_cny_exr is null then 1 else t2.cnv_cny_exr end as cnv_cny_exr
            from
            (select * from {0} where batch = '{2}') t1
            left outer join
            (select currency, cnv_cny_exr from {1} where dw_dat_dt = '{3}') t2
            on
                t1.currency = t2.currency
            '''.format(base_table_guarantee, table_exchange_rate, today_date, yesterday_date)
        ).createOrReplaceTempView('guarantee_rel_with_currency')

        spark.sql(
            '''
            select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, cast(cnv_cny_guaranteeamount as decimal(18,2)), cnv_cny_exr
            from
            (select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, batch, cnv_cny_exr, 
            guaranteeamount * cnv_cny_exr as cnv_cny_guaranteeamount
            from guarantee_rel_with_currency)
            where grnt_custid != bor_custid
            '''
        ).createOrReplaceTempView('guarantee_rel_table')

        spark.sql('cache table guarantee_tag_table')
        spark.sql('cache table guarantee_rel_table')

        v = spark.sql('select node_id as id from guarantee_tag_table').distinct()

        v.persist(StorageLevel.MEMORY_AND_DISK_SER)

        e = spark.sql(
            '''
            select grnt_custid as src , bor_custid as dst, 'guarantee' as relationship
            from guarantee_rel_table
            where grnt_custid != '' and bor_custid != '' and grnt_custid != bor_custid
            ''').distinct()

        e.persist(StorageLevel.MEMORY_AND_DISK_SER)

        graph = GraphFrame(v, e)
        graph.connectedComponents().createOrReplaceTempView('cc_df')
        e.createOrReplaceTempView('e')
        spark.sql(
            '''
            select t2.src, t2.dst, cast(t1.component as string)
            from
            cc_df t1
            left outer join
            e t2
            on t1.id = t2.src
            where t2.src is not null and t2.dst is not null
            ''').distinct().createOrReplaceTempView('v_1')

        spark.sql(
            '''
            select
                t1.src,
                t1.dst,
                named_struct(
                    "dw_stat_dt", cast(t2.dw_stat_dt as string),
                    "grnt_cod", t2.grnt_cod,
                    "grnt_custid", t2.grnt_custid,
                    "grnt_nm", t2.grnt_nm,
                    "bor_cod", t2.bor_cod,
                    "bor_custid", t2.bor_custid,
                    "bor_nm", t2.bor_nm,
                    "grnt_ctr_id", t2.grnt_ctr_id,
                    "guaranteetype", t2.guaranteetype,
                    "currency", t2.currency,
                    "guaranteeamount", cast(t2.guaranteeamount as string),
                    "sgn_dt", cast(t2.sgn_dt as string),
                    "data_src", t2.data_src,
                    "cnv_cny_guaranteeamount", cast(t2.cnv_cny_guaranteeamount as string)
                ) as attr,
                t1.component
            from
                (select * from v_1) t1
                left outer join
                (select * from guarantee_rel_table) t2
                on t1.src = t2.grnt_custid and t1.dst = t2.bor_custid
                where t2.grnt_custid is not null and t2.bor_custid is not null
            ''').createOrReplaceTempView('v_component_with_attr')

        spark.sql(
            '''
            select named_struct(
            "link", array(src, dst),
            "attrs", attrs) as link,
            component
            from
            (select src, dst, collect_list(attr) as attrs, component
            from v_component_with_attr
            group by src, dst, component)
            '''
        ).createOrReplaceTempView('v_component')

        print('===============connected components finished===================')


if __name__ == '__main__':
    try:
        spark = SparkSession \
            .builder \
            .appName('guarantee_relationship') \
            .config('spark.log.level', 'WARN') \
            .enableHiveSupport() \
            .getOrCreate()

        t_guarantee = BASE_SPARK.get_guarantee_table()  # guarantee relationship table
        t_base_guarantee = BASE_SPARK.get_base_guarantee_table()  # materialize guarantee relationship table
        t_custid_eid = BASE_SPARK.get_custid_eid_table()  # customer id maps eid table
        t_base_custid_eid = BASE_SPARK.get_base_custid_eid_table()  # materialize customer id maps eid table
        t_exchange_rate = BASE_SPARK.get_exchange_table()  # exchange rate table
        check_point_dir = BASE_SPARK.get_check_point_path()  # check point dir

        output_small_graph_table = BASE_SPARK.get_output_small_graph_table()  # small graph hive table

        spark.sparkContext.setCheckpointDir(check_point_dir)

        components = ConnectedComponents(t_guarantee, t_base_guarantee, t_custid_eid, t_base_custid_eid,
                                         t_exchange_rate)
        components.materialize()
        components.connected_components()

        spark.sql(
            '''
            select component, collect_list(link) as links
            from v_component
            group by component
            '''
        ).createOrReplaceTempView('v_links')

        spark.sql('''select component, links from v_links''').createOrReplaceTempView('grnt_graph')
        spark.table('grnt_graph').write.mode('overwrite').json(BASE_SPARK.get_hdfs_graph_file_path())

        spark.sql(
            '''
            select explode(t2.link) as node_id
            from
            grnt_graph t1
            left outer join
            (select link.link as link, component from v_component) t2
            on t1.component = t2.component
            where t2.component is not null
            '''
        ).distinct().createOrReplaceTempView('graph_nodes')

        spark.sql(
            '''
            select t2.node_id, named_struct("node_id", t2.node_id, "eid", t2.eid, "node_type", t2.node_type,
                "node_name", t2.node_name) as node
            from
            graph_nodes t1
            left outer join
            guarantee_tag_table t2
            on t1.node_id = t2.node_id
            where t2.node_id is not null and t2.node_id != ''
            '''
        ).createOrReplaceTempView('graph_nodes_with_attr')

        spark.table('graph_nodes_with_attr').write.mode('overwrite').json(BASE_SPARK.get_hdfs_nodes_file_path())

        del components
    except Exception as ex:
        print(ex)
