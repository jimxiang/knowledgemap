# /usr/bin/env/ python
# -*- coding: utf-8 -*-
import sys
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from datetime import datetime, timedelta
import logging
from config import BASE_SPARK

sys.path.append(BASE_SPARK.get_graphframes_path())
from graphframe import GraphFrame

reload(sys)
sys.setdefaultencoding('utf-8')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


def get_date_before_today():
    today = datetime.now()
    delta = timedelta(days=-1)
    yesterday = today + delta
    return yesterday.strftime('%Y-%m-%d')


def connected_components(base_table_guarantee, base_table_custid_eid, table_exchange_rate):
    grnt_latest_batch = str(spark.sql("select max(batch) batch from %s" % base_table_guarantee).take(1)[0]['batch'])
    custid_latest_batch = str(spark.sql("select max(batch) batch from %s" % base_table_custid_eid).take(1)[0]['batch'])
    exchange_max_batch = str(
        spark.sql("select cast(max(dw_dat_dt) as string) batch from %s" % table_exchange_rate).take(1)[0]['batch'])

    if exchange_max_batch == yesterday_date:
        exchange_batch = yesterday_date
    else:
        exchange_batch = exchange_max_batch

    # filter out data without customer id and name
    spark.sql(
        '''
        select * from {base_table_guarantee} where batch = '{batch}' and 
        !((grnt_custid = '' or grnt_custid is null) and (grnt_nm = '' or grnt_nm is null)) and 
        !((bor_custid = '' or bor_custid is null) and (bor_nm = '' or bor_nm is null))
        '''.format(base_table_guarantee=base_table_guarantee, batch=grnt_latest_batch)
    ).createOrReplaceTempView('base_guarantee_relation')

    spark.sql(
        '''
        select t1.grnt_cod as node_id, t2.eid, 
            case when t1.grnt_custid is not null then substr(t1.grnt_custid, 0, 2) else 'UNKNOWN' end as node_type, 
            t1.grnt_nm as node_name
        from 
        (select * from base_guarantee_relation where batch = '{grnt_batch}') t1
        left outer join
        (select * from {t_custid_eid} where batch = '{custid_batch}') t2
        on t1.grnt_custid = t2.cust_id
        union
        select t3.bor_cod as node_id, t4.eid, 
            case when t3.bor_custid is not null then substr(t3.bor_custid, 0, 2) else 'UNKNOWN' end as node_type, 
            t3.bor_nm as node_name
        from
        (select * from base_guarantee_relation where batch = '{grnt_batch}') t3
        left outer join
        (select * from {t_custid_eid} where batch = '{custid_batch}') t4
        on t3.bor_custid = t4.cust_id
        '''.format(grnt_batch=grnt_latest_batch, t_custid_eid=base_table_custid_eid, custid_batch=custid_latest_batch)
    ).createOrReplaceTempView('guarantee_tag')

    spark.sql(
        '''
        select
            a.node_id,
            case
                when a.eid is not null then a.eid
                when a.eid is null and a.node_type = 'CM' then b.eid
                when a.eid is null and a.node_type = 'UNKNOWN' then b.eid
            end as eid,
            a.node_type,
            a.node_name
        from guarantee_tag a
        left outer join
        (select eid, name from enterprises.hive_enterprises) b
        on a.node_name = b.name
        ''').distinct().createOrReplaceTempView("guarantee_tag_table")

    spark.sql(
        '''
        select t1.*, case when t2.cnv_cny_exr is null then 1 else t2.cnv_cny_exr end as cnv_cny_exr
        from
        (select * from base_guarantee_relation where batch = '{grnt_batch}') t1
        left outer join
        (select currency, cnv_cny_exr from {t_ex_rate} where dw_dat_dt = '{exchange_batch}') t2
        on
            t1.currency = t2.currency
        '''.format(grnt_batch=grnt_latest_batch, t_ex_rate=table_exchange_rate, exchange_batch=exchange_batch)
    ).createOrReplaceTempView('guarantee_rel_with_currency')

    spark.sql(
        '''
        select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, cast(cnv_cny_guaranteeamount as decimal(18,2)), 
            cast(cnv_cny_exr as decimal(18, 2))
        from
        (select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, batch, cnv_cny_exr, 
            guaranteeamount * cnv_cny_exr as cnv_cny_guaranteeamount
        from guarantee_rel_with_currency)
        ''').createOrReplaceTempView('guarantee_rel_table')

    spark.sql('cache table guarantee_tag_table')
    spark.sql('cache table guarantee_rel_table')

    v = spark.sql('select node_id as id from guarantee_tag_table').distinct()

    v.persist(StorageLevel.MEMORY_AND_DISK_SER)

    e = spark.sql(
        '''
        select grnt_cod as src , bor_cod as dst, 'guarantee' as relationship
        from guarantee_rel_table
        where grnt_cod != '' and bor_cod != '' and grnt_cod != bor_cod
        ''').distinct()

    e.persist(StorageLevel.MEMORY_AND_DISK_SER)

    graph = GraphFrame(v, e)
    graph.connectedComponents().createOrReplaceTempView('cc_df')
    cc_count = spark.table('cc_df').select('component').distinct().count()
    logging.info('Connected components finished! Count: %d' % cc_count)
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
            on t1.src = t2.grnt_cod and t1.dst = t2.bor_cod
            where t2.grnt_cod is not null and t2.bor_cod is not null
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

    spark.sql(
        '''
        select component, collect_list(link) as links
        from v_component
        group by component
        '''
    ).createOrReplaceTempView('v_links')

    spark.sql('''select component, links from v_links''').createOrReplaceTempView('grnt_graph')
    spark.table('grnt_graph').write.mode('overwrite').json(BASE_SPARK.get_hdfs_graph_file_path())
    logging.info('Write graph file finished')

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
    logging.info('Write nodes file finished')


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('guarantee_relation') \
        .config('spark.log.level', 'WARN') \
        .enableHiveSupport() \
        .getOrCreate()

    logging.info('Process start')

    t_base_guarantee = BASE_SPARK.get_base_guarantee_table()  # materialize guarantee relationship table
    t_base_custid_eid = BASE_SPARK.get_base_custid_eid_table()  # materialize customer id maps eid table
    t_exchange_rate = BASE_SPARK.get_exchange_table()  # exchange rate table
    check_point_dir = BASE_SPARK.get_check_point_path()  # check point dir

    yesterday_date = get_date_before_today()

    spark.sparkContext.setCheckpointDir(check_point_dir)

    connected_components(t_base_guarantee, t_base_custid_eid, t_exchange_rate)
    logging.info('Process end')
