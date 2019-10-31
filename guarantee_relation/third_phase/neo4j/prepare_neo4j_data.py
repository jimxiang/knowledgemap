# /usr/bin/env/ python
# -*- coding: utf-8 -*-
import ConfigParser
import sys
import logging
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel
from utils import Utils

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')

env = 'dev'
conf_path = '../config/' + env
logging.info('conf path: %s' % conf_path)
cfg = ConfigParser.SafeConfigParser()
cfg.read(conf_path + '/guarantee.cfg')

reload(sys)
sys.setdefaultencoding('utf-8')


def prepare_neo4j_data():
    base_table_guarantee = cfg.get('hivetable', 'guarantee_relation_base_table')
    base_table_custid_eid = cfg.get('hivetable', 'custid_eid_base_table')
    table_exchange_rate = cfg.get('hivetable', 'exchange_rate_table')
    table_enterprises = cfg.get('hivetable', 'enterprises_table')
    table_corp_map = cfg.get('hivetable', 'corp_map_table')

    yesterday_date = Utils.get_yesterday_date()
    grnt_latest_batch = str(spark.sql("select max(batch) batch from %s" % base_table_guarantee).take(1)[0]['batch'])
    custid_latest_batch = str(spark.sql("select max(batch) batch from %s" % base_table_custid_eid).take(1)[0]['batch'])
    exchange_max_batch = str(
        spark.sql("select cast(max(dw_dat_dt) as string) batch from %s" % table_exchange_rate).take(1)[0]['batch'])

    if exchange_max_batch == yesterday_date:
        exchange_batch = yesterday_date
    else:
        exchange_batch = exchange_max_batch

    scheme = StructType([
        StructField('corp_name', StringType(), True),
        StructField('corp_id', StringType(), True)
    ])
    spark.udf.register('parse_corp_info', Utils.parse_corp_info, scheme)
    spark.udf.register('gen_uuid', Utils.gen_uuid, StringType())
    spark.udf.register('json_to_string', Utils.json_to_string, StringType())

    # filter out data without customer id and name
    spark.sql(
        '''
        select * from {base_table_guarantee} where batch = '{batch}' and 
        !((grnt_custid = '' or grnt_custid is null) and (grnt_nm = '' or grnt_nm is null)) and 
        !((bor_custid = '' or bor_custid is null) and (bor_nm = '' or bor_nm is null))
        '''.format(base_table_guarantee=base_table_guarantee, batch=grnt_latest_batch)
    ).createOrReplaceTempView('base_guarantee_relation')

    # === Nodes start ===

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
    ).createOrReplaceTempView('t_nodes')

    # Supplement eid for nodes by fully matching company name
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
        from
        t_nodes a
        left outer join
        (select eid, name from {t_enterprises}) b
        on a.node_name = b.name
        '''.format(t_enterprises=table_enterprises)).distinct().createOrReplaceTempView('t_nodes_eid')

    # Add corporation tag for nodes
    spark.sql(
        '''
        select t.eid, t.corp_info.corp_name corp_name, t.corp_info.corp_id corp_id
         from
         (select `qixinbao_id|string` eid, parse_corp_info(`corp_list|string`) corp_info from %s) t
        ''' % table_corp_map).createOrReplaceTempView('t_corp')
    spark.sql(
        '''
        select a.*, b.corp_name, b.corp_id
        from
        t_nodes_eid a
        left outer join
        t_corp b
        on a.eid = b.eid
        ''').createOrReplaceTempView('guarantee_corp')

    spark.sql(
        '''
        select corp_id, count(*) corp_cnt, concat_ws(';', (collect_set(node_id))) corp_list
        from guarantee_corp
        where corp_id is not null
        group by corp_id
        ''').createOrReplaceTempView('corp_info')

    spark.sql(
        '''
        select a.*, b.corp_cnt
        from
        guarantee_corp a
        left outer join
        corp_info b
        on a.corp_id = b.corp_id
        ''').createOrReplaceTempView('guarantee_nodes')

    spark.sql(
        '''
        select b.*, a.corp_name, b.corp_id node_id
        from
        (select corp_id, corp_name from guarantee_corp where corp_id is not null) a
        left outer join
        corp_info b
        on a.corp_id = b.corp_id
        where b.corp_id is not null and b.corp_cnt > 1
        ''').distinct().createOrReplaceTempView('corp_nodes')

    # === Nodes end ===

    # === Edges start ===

    spark.sql(
        '''
        select t1.*, case when t2.cnv_cny_exr is null then 1 else t2.cnv_cny_exr end as cnv_cny_exr
        from
        (select * from base_guarantee_relation where batch = '{grnt_batch}') t1
        left outer join
        (select currency, cnv_cny_exr from {t_ex_rate} where dw_dat_dt = '{exchange_batch}') t2
        on t1.currency = t2.currency
        '''.format(grnt_batch=grnt_latest_batch, t_ex_rate=table_exchange_rate, exchange_batch=exchange_batch)
    ).createOrReplaceTempView('t_rel')

    spark.sql(
        '''
        select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, cast(cnv_cny_guaranteeamount as decimal(18,2)), 
            cast(cnv_cny_exr as decimal(18, 2))
        from
        (select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, batch, cnv_cny_exr, 
            guaranteeamount * cnv_cny_exr as cnv_cny_guaranteeamount
        from t_rel)
        where grnt_cod != bor_cod
        ''').createOrReplaceTempView('t_rel_cny')

    spark.sql(
        '''
        select grnt_cod, bor_cod, grnt_nm, bor_nm, map(
            "dw_stat_dt", cast(dw_stat_dt as string), 
            "grnt_custid", grnt_custid,
            "bor_custid", bor_custid,
            "grnt_ctr_id", grnt_ctr_id,
            "guaranteetype", guaranteetype, 
            "currency", currency, 
            "guaranteeamount", guaranteeamount, 
            "sgn_dt", cast(sgn_dt as string), 
            "data_src", data_src,
            "cnv_cny_guaranteeamount", cnv_cny_guaranteeamount,
            "cnv_cny_exr",cnv_cny_exr
        ) as attr, cnv_cny_guaranteeamount
        from t_rel_cny
        ''').createOrReplaceTempView('t_rel_concat_attr')

    spark.sql(
        '''
        select grnt_cod, bor_cod, grnt_nm as src_name, bor_nm as dst_name, 
            concat_ws(";", collect_list(json_to_string(attr))) as attrs, count(*) as times,
            sum(cnv_cny_guaranteeamount) as amount
        from t_rel_concat_attr
        group by grnt_cod, bor_cod, grnt_nm, bor_nm
        ''').createOrReplaceTempView('t_grnt_rel')

    # build graph
    v = spark.sql('select node_id as id from guarantee_nodes').distinct()

    v.persist(StorageLevel.MEMORY_AND_DISK_SER)

    e = spark.sql(
        '''
        select grnt_cod as src , bor_cod as dst, 'guarantee' as relationship
        from t_rel_cny
        where grnt_cod != '' and bor_cod != '' and grnt_cod != bor_cod
        ''').distinct()

    e.persist(StorageLevel.MEMORY_AND_DISK_SER)

    sys.path.append(cfg.get('graphframes', 'graphframes_path'))
    from graphframe import GraphFrame
    graph = GraphFrame(v, e)
    graph.find('(a)-[e1]->(b); (b)-[e2]->(a)') \
        .selectExpr('a.id src', 'b.id dst', 'gen_uuid(a.id, b.id) circle_id', '2 rel_type') \
        .createOrReplaceTempView('each_other')

    # Add attribute for edges
    spark.sql(
        '''
        select a.*, b.circle_id, case when b.rel_type is not null then b.rel_type else 1 end as rel_type
        from
        t_grnt_rel a
        left outer join
        each_other b
        on (a.grnt_cod = b.src and a.bor_cod = b.dst)
        ''').createOrReplaceTempView('guarantee_edges')

    spark.sql(
        '''
        select node_id, corp_id, node_name, corp_name
        from guarantee_nodes
        where corp_id is not null and corp_cnt > 1
        ''').distinct().createOrReplaceTempView('belong_rel')

    spark.sql('select grnt_cod src, bor_cod dst from guarantee_edges').createOrReplaceTempView('grnt_rel')

    spark.sql(
        '''
        select a.node_id src, a.corp_id src_corp_id, c.node_id dst, c.corp_id dst_corp_id, a.corp_name as src_name,
            c.corp_name as dst_name
        from
        belong_rel a
        left outer join
        grnt_rel b
        on a.node_id = b.src
        left outer join
        belong_rel c
        on b.dst = c.node_id
        where c.node_id is not null
        ''').createOrReplaceTempView('corp_rel_temp')

    spark.sql(
        '''
        select a.*, b.times, b.amount
        from
        corp_rel_temp a
        left outer join
        guarantee_edges b
        on a.src = b.grnt_cod and a.dst = b.bor_cod
        where b.grnt_cod is not null and b.bor_cod is not null
        ''').createOrReplaceTempView('corp_rel_detail')

    spark.sql(
        '''
        select src_corp_id, dst_corp_id, src_name, dst_name, sum(amount) amount, sum(times) times
        from corp_rel_detail
        group by src_corp_id, dst_corp_id, src_name, dst_name
        ''').createOrReplaceTempView('corp_rel')

    # edges for guarantee entity and corp entity
    v_2 = spark.sql(
        '''
        select node_id as id, 'guarantee' as type from guarantee_nodes
        union 
        select corp_id as id, 'corp' as type from corp_nodes where corp_cnt > 1
        ''').distinct()

    v_2.persist(StorageLevel.MEMORY_AND_DISK_SER)

    e_2 = spark.sql(
        '''
        select grnt_cod as src , bor_cod as dst, 'guarantee_rel' as relationship, grnt_nm as name_a, 
            bor_nm as name_b
        from t_rel_cny
        where grnt_cod != '' and bor_cod != '' and grnt_cod != bor_cod
        union
        select node_id as src, corp_id as dst, 'belong_rel' as relationship, node_name as name_a, corp_name as name_b
        from belong_rel
        ''').distinct()

    e_2.persist(StorageLevel.MEMORY_AND_DISK_SER)

    graph_2 = GraphFrame(v_2, e_2)
    graph_2.find('(a)-[e1]->(b); (b)-[e2]->(c)')\
        .filter('e1.relationship = "guarantee_rel" and e2.relationship = "belong_rel"')\
        .selectExpr('a.id as entity_id', 'b.id as entity_fd_id', 'c.id as corp_id', 'e1.name_a as src_name', 'e2.name_b as dst_name')\
        .createOrReplaceTempView('entity_to_corp_part1')

    graph_2.find('(b)-[e1]->(a); (b)-[e2]->(c)') \
        .filter('e1.relationship = "guarantee_rel" and e2.relationship = "belong_rel"') \
        .selectExpr('a.id as entity_id', 'b.id as entity_fd_id', 'c.id as corp_id', 'e2.name_b as src_name', 'e1.name_b as dst_name') \
        .createOrReplaceTempView('entity_to_corp_part2')

    spark.sql(
        '''
        select a.entity_fd_id, b.times, b.amount, a.entity_id src, a.corp_id dst, a.src_name, a.dst_name
        from
        entity_to_corp_part1 a
        left outer join
        guarantee_edges b
        on a.entity_id = b.grnt_cod and a.entity_fd_id = b.bor_cod
        where b.grnt_cod is not null and b.bor_cod is not null
        union
        select a.entity_fd_id, b.times, b.amount, a.corp_id src, a.entity_id dst, a.src_name, a.dst_name
        from
        entity_to_corp_part2 a
        left outer join
        guarantee_edges b
        on a.entity_fd_id = b.grnt_cod and a.entity_id = b.bor_cod
        where b.grnt_cod is not null and b.bor_cod is not null
        ''').createOrReplaceTempView('entity_to_corp_info')

    spark.sql(
        '''
        select src, dst, src_name, dst_name, collect_set(entity_fd_id) as first_degree_ids, sum(times) as times, 
            sum(amount) as amount
        from entity_to_corp_info
        group by src, dst, src_name, dst_name
        ''').createOrReplaceTempView('entity_to_corp_agg')

    spark.sql(
        '''
        select a.entity_id src, a.entity_fd_id dst
        from
        entity_to_corp_part1 a
        left outer join
        guarantee_edges b
        on a.entity_id = b.grnt_cod and a.entity_fd_id = b.bor_cod
        where b.grnt_cod is not null and b.bor_cod is not null
        union
        select a.entity_fd_id src, a.entity_id dst
        from
        entity_to_corp_part2 a
        left outer join
        guarantee_edges b
        on a.entity_fd_id = b.grnt_cod and a.entity_id = b.bor_cod
        where b.grnt_cod is not null and b.bor_cod is not null
        ''').createOrReplaceTempView('entity_related_to_corp')

    spark.sql(
        '''
        select a.*
        from
        guarantee_edges a
        left outer join
        entity_related_to_corp b
        on a.grnt_cod = b.src and a.bor_cod = b.dst
        where b.src is null and b.dst is null
        ''').createOrReplaceTempView('entity_to_entity')

    # === Edges end ===

    # Nodes CSV
    nodes_guarantee_csv = cfg.get('csv', 'nodes_guarantee')
    nodes_corp_csv = cfg.get('csv', 'nodes_corp')
    # Edges CSV
    edges_guarantee_csv = cfg.get('csv', 'edges_guarantee')
    edges_belong_csv = cfg.get('csv', 'edges_belong')
    edges_corp_csv = cfg.get('csv', 'edges_corp')
    edges_corp_agg_csv = cfg.get('csv', 'edges_corp_agg')

    # Filter null value

    spark.sql(
        '''
        select node_id `node_id:ID`, eid, node_type, node_name, corp_name, corp_id, corp_cnt
        from guarantee_nodes
        ''').write.mode('overwrite').option('sep', '\001').csv(nodes_guarantee_csv)
    logging.info('Write nodes guarantee csv finished!')

    spark.sql(
        '''
        select corp_id `corp_id:ID`, corp_cnt, corp_list `corp_list:string[]`, corp_name, node_id, 'CO' node_type
        from corp_nodes
        ''').write.mode('overwrite').option('sep', '\001').csv(nodes_corp_csv)
    logging.info('Write nodes corp csv finished!')

    spark.sql(
        '''
        select grnt_cod `grnt_cod:START_ID`, bor_cod `bor_cod:END_ID`, grnt_cod src, bor_cod dst,
            attrs `attrs:string[]`, case when circle_id is null then '' else circle_id end circle_id, rel_type, times,
            amount, src_name, dst_name
        from guarantee_edges
        ''').write.mode('overwrite').option('sep', '\001').csv(edges_guarantee_csv)
    logging.info('Write edges guarantee csv finished!')

    spark.sql(
        '''
        select node_id `node_id:START_ID`, corp_id `corp_id:END_ID`, node_name src_name, corp_name dst_name
        from belong_rel
        ''').write.mode('overwrite').option('sep', '\001').csv(edges_belong_csv)
    logging.info('Write edges belong csv finished!')

    spark.sql(
        '''
        select src_corp_id `src_corp_id:START_ID`, dst_corp_id `dst_corp_id:END_ID`, src_corp_id src, dst_corp_id dst, 
            '' as circle_id, 4 as rel_type, amount, times, src_name, dst_name
        from corp_rel
        ''').write.mode('overwrite').option('sep', '\001').csv(edges_corp_csv)
    logging.info('Write edges corp csv finished!')

    spark.sql(
        '''
        select src `src:START_ID`, dst `dst:END_ID`, src, dst, concat_ws(";", first_degree_ids) `node_ids:string[]`,
            times, amount, 3 rel_type, src_name, dst_name
        from entity_to_corp_agg
        ''').write.mode('overwrite').option('sep', '\001').csv(edges_corp_agg_csv)
    logging.info('Write edges corp agg csv finished!')


def main():
    prepare_neo4j_data()


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName('guarantee_relation') \
        .config('spark.log.level', 'WARN') \
        .enableHiveSupport() \
        .getOrCreate()

    main()
