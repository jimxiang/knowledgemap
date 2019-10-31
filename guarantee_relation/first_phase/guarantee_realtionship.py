# /usr/bin/env/ python
# -*- coding: utf-8 -*-
import os
import random
import sys
import networkx as nx
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import Row
from pyspark.storagelevel import StorageLevel
from datetime import datetime, timedelta
import json

sys.path.append('/xxx/graphframes')
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
            lastest_batch = str(sql_context.sql("select max(batch) batch from %s" % table_guarantee).take(1)[0]['batch'])
            sql_context.sql("select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, "
                            "grnt_ctr_id, guaranteetype,currency, guaranteeamount, sgn_dt, data_src, grnt_mapid, "
                            "bor_mapid from %s where batch = '%s'" % (table_guarantee, lastest_batch))\
                .createOrReplaceTempView('base_table_guarantee')
            sql_context.sql(
                '''
                insert overwrite table %s partition(batch='%s') select * from base_table_guarantee
                ''' % (base_table_guarantee, today_date))

            sql_context.sql('''select cvm_cust_id as cust_id, eid from %s''' % table_custid_eid) \
                .createOrReplaceTempView("base_table_custid_eid")
            sql_context.sql(
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

        sql_context.sql(
            '''
            select t1.grnt_custid as id, t2.eid as qixinbao_id, 
                substr(t1.grnt_custid, 0, 2) as entity_type, t1.grnt_nm as entity_name
            from 
            (select * from {0} where batch = '{2}') t1
            left outer join
            (select * from {1} where batch = '{2}') t2
            on t1.grnt_custid = t2.cust_id
            union
            select t3.bor_custid as id, t4.eid as qixinbao_id, 
                substr(t3.bor_custid, 0, 2) as entity_type, t3.bor_nm as entity_name
            from
            (select * from {0} where batch = '{2}') t3
            left outer join
            (select * from {1} where batch = '{2}') t4
            on t3.bor_custid = t4.cust_id
            '''.format(base_table_guarantee, base_table_custid_eid, today_date)
        ).createOrReplaceTempView('guarantee_tag')
        
        sql_context.sql("select a.id, case when a.qixinbao_id is not null then a.qixinbao_id when a.entity_type = 'CM' "
                        "then b.eid end as qixinbao_id, a.entity_type, a.entity_name from guarantee_tag a "
                        "left outer join (select eid, name from enterprises.hive_enterprises) b "
                        "on a.entity_name = b.name").distinct().createOrReplaceTempView("guarantee_tag_table")

        sql_context.sql(
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

        sql_context.sql(
            '''
            select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, cast(cnv_cny_guaranteeamount as decimal(18,2)), cnv_cny_exr
            from
            (select dw_stat_dt, grnt_cod, grnt_custid, grnt_nm, bor_cod, bor_custid, bor_nm, grnt_ctr_id, guaranteetype,
            currency, guaranteeamount, sgn_dt, data_src, batch, cnv_cny_exr, 
            guaranteeamount * cnv_cny_exr as cnv_cny_guaranteeamount
            from guarantee_rel_with_currency)
            '''
        ).createOrReplaceTempView('guarantee_rel_table')
        
        sql_context.sql('cache table guarantee_tag_table')
        sql_context.sql('cache table guarantee_rel_table')
        
        v = sql_context.sql(
            '''
            select id
            from guarantee_tag_table
            ''').distinct()

        v.persist(StorageLevel.MEMORY_AND_DISK_SER)

        e = sql_context.sql(
            '''
            select grnt_custid as src , bor_custid as dst, 'guarantee' as relationship
            from guarantee_rel_table
            ''').distinct()

        e.persist(StorageLevel.MEMORY_AND_DISK_SER)

        graph = GraphFrame(v, e)
        cc_df = graph.connectedComponents()
        cond = [cc_df.id == e.src]
        cc_df.join(e, cond, 'outer') \
            .select(e.src, e.dst, cc_df.component) \
            .filter(e.src != 'null') \
            .filter(e.dst != 'null') \
            .selectExpr('array(src, dst) as link', 'component') \
            .distinct() \
            .createOrReplaceTempView('v_component')
        
        sql_context.sql("select link, cast(component as string) from v_component").createOrReplaceTempView('v_component')
        print('===============connected components finished===================')


def all_paths(edges):
    try:
        edges = [(x[0], x[1]) for x in edges]
        sub_graph = nx.DiGraph(edges)
        all_nodes = [n for n in sub_graph.nodes()]
        all_pairs_links = nx.all_pairs_shortest_path(sub_graph)
        all_cycles = nx.simple_cycles(sub_graph)

        all_links = [path[1][key] for path in all_pairs_links for key in path[1] if len(path[1][key]) == 2]
        cycles = [cycle for cycle in all_cycles]
        cycle_pairs = []
        for cycle in cycles:
            for c in range(len(cycle)):
                t = []
                if c == len(cycle) - 1:
                    t.append(cycle[c])
                    t.append(cycle[0])
                else:
                    t.append(cycle[c])
                    t.append(cycle[c + 1])
                cycle_pairs.append(''.join(t))

        links = [link for link in all_links if (''.join(link)) not in cycle_pairs]

        link_list = [{'source_id': str(link[0]), 'target_id': str(link[1])} for link in links]

        link_results = {'paths': link_list, 'type': 'link'}

        cycle_list = []
        for cycle in cycles:
            temp_list = []
            if len(cycle) == 2:
                source_id = str(cycle[0])
                target_id = str(cycle[1])
                if sub_graph.has_edge(source_id, target_id):
                    temp = {'source_id': source_id, 'target_id': target_id}
                    temp_list.append(temp)
                source_id = str(cycle[1])
                target_id = str(cycle[0])
                if sub_graph.has_edge(source_id, target_id):
                    temp = {'source_id': source_id, 'target_id': target_id}
                    temp_list.append(temp)

                cycle_list.append(temp_list)
            else:
                for i in range(len(cycle)):
                    if i == (len(cycle) - 1):
                        source_id = str(cycle[i])
                        target_id = str(cycle[0])
                        if sub_graph.has_edge(source_id, target_id):
                            temp = {'source_id': source_id, 'target_id': target_id}
                            temp_list.append(temp)
                    else:
                        source_id = str(cycle[i])
                        target_id = str(cycle[i + 1])
                        if sub_graph.has_edge(source_id, target_id):
                            temp = {'source_id': source_id, 'target_id': target_id}
                            temp_list.append(temp)

                cycle_list.append(temp_list)

        cycle_dict_list = [
            {'paths': cycle,
             'type': 'circle' if len(cycle) > 2 else 'each_other' if len(cycle) == 2 else 'self'
             } for cycle in cycle_list]
        
        result = {"nodes": all_nodes, "links": link_results, "cycles": cycle_dict_list}
        return result

    except nx.NetworkXException:
        return []


def map_string(line):
    line = json.loads(line)
    del line['id']
    line = json.dumps(line['relation_list']).decode("unicode_escape")
    return line


def random_id(uid):
    suffix = random.randint(1, 10000)
    return ''.join([uid, '-', str(suffix)])


if __name__ == '__main__':
    try:
        main_params = dict()
        if len(sys.argv) > 2:
            for opt in sys.argv[1:]:
                kv = opt.split('=', 1)
                if len(kv) == 2:
                    if kv[0][:2] != '--':
                        print('invalid flag ' + kv[0])
                        sys.exit(2)
                    main_params[kv[0][2:]] = kv[1]

        sql_context = SparkSession \
            .builder \
            .appName('guarantee_relationship') \
            .config('spark.log.level', 'WARN') \
            .enableHiveSupport() \
            .getOrCreate()

        output_table = main_params.get('output_table', '')  # result output table
        t_guarantee = main_params.get('grnt_table', '')  # guarantee relationship table
        t_base_guarantee = main_params.get('grnt_base_table', '')  # materialize guarantee relationship table
        t_custid_eid = main_params.get('custid_eid_table', '')  # customer id maps eid table
        t_base_custid_eid = main_params.get('custid_eid_base_table', '')  # materialize customer id maps eid table
        t_exchange_rate = main_params.get('exchange_rate_table', '')  # exchange rate table
        check_point_dir = main_params.get('check_point_dir', '')  # check point dir

        sql_context.sparkContext.setCheckpointDir(check_point_dir)

        components = ConnectedComponents(t_guarantee, t_base_guarantee, t_custid_eid, t_base_custid_eid,
                                         t_exchange_rate)

        components.materialize()
        components.connected_components()

        sql_context.udf.register('all_paths', all_paths, StructType([
            StructField("nodes", ArrayType(StringType()), True),
            StructField("cycles", ArrayType(
                StructType([
                    StructField("paths", ArrayType(
                        StructType([
                            StructField("source_id", StringType(), True),
                            StructField("target_id", StringType(), True)
                        ])
                    ), True),
                    StructField("type", StringType(), True)
                ])
            )),
            StructField("links", StructType([
                StructField("paths", ArrayType(
                    StructType([
                        StructField("source_id", StringType(), True),
                        StructField("target_id", StringType(), True)
                    ])
                ), True),
                StructField("type", StringType(), True)
            ]))
        ]))

        sql_context.udf.register("map_string", map_string, StringType())

        sql_context.udf.register("random_id", random_id, StringType())

        sql_context.sql('select component, collect_set(link) as links from v_component group by component') \
            .createOrReplaceTempView('v_links')

        sql_context.sql('cache table v_links')
        sql_context.sql('select component, links from v_links where size(links) < 300')\
            .createOrReplaceTempView('v_links_less_then_300')
        sql_context.sql('select all_paths(links) as data, component from v_links_less_then_300')\
            .createOrReplaceTempView('v_paths_filter')
        
        sql_context.sql('cache table v_paths_filter')

        sql_context.sql('select explode(data.nodes) as id, data.nodes as cycles, data.links as links, component from '
                        'v_paths_filter').createOrReplaceTempView('v_explode_nodes')
                        
        sql_context.sql('select id, explode(cycles) as paths, component, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as uuid from '
                        'v_explode_nodes union all select id, links as paths, component, regexp_replace(reflect("java.util.UUID", "randomUUID"), "-", "") as uuid from v_explode_nodes''').createOrReplaceTempView('v_explode_paths_with_uuid')

        sql_context.sql(
            '''
            select paths.id as id, explode(paths.paths) as link, paths.type, component, uuid from v_explode_paths_with_uuid
            ''').createOrReplaceTempView('v_paths_explode')

        sql_context.sql("select random_id(id) as id, link, type, component, uuid from v_paths_explode")

        sql_context.sql(
            '''
            select split(t1.id, '-')[0] as id,
                t1.link['source_id'] as source_id,
                t1.link['target_id'] as target_id,
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
                    "guaranteeamount", t2.guaranteeamount,
                    "sgn_dt", cast(t2.sgn_dt as string),
                    "data_src", t2.data_src,
                    "cnv_cny_guaranteeamount", t2.cnv_cny_guaranteeamount
                ) as link,
                t1.component,
                t1.type,
                t1.uuid
            from
                (select * from v_paths_explode) t1
                left outer join
                (select * from guarantee_rel_table) t2
                on t1.link['source_id'] = t2.grnt_custid and t1.link['target_id'] = t2.bor_custid
                where t2.grnt_custid is not null and t2.bor_custid is not null
            ''').createOrReplaceTempView('v_links_with_attr')
        # Collect links that the same src and dst have more than one guarantee events.
        sql_context.sql(
            '''
            select id,
                named_struct(
                    "source_id", source_id,
                    "target_id", target_id,
                    "attrs", collect_set(link)) as link,
                component,
                type,
                uuid
            from v_links_with_attr
            group by id, source_id, target_id, type, uuid, component
            ''').createOrReplaceTempView('v_links_collect_attr')

        sql_context.sql(
            '''
            select id, collect_set(link) as paths, type from v_links_collect_attr group by id, component, type, uuid
            ''').distinct().createOrReplaceTempView('v_paths_with_attr')

        sql_context.sql(
            '''
            select
                id,
                named_struct(
                    "type", type,
                    "paths", paths) as item
            from v_paths_with_attr
            ''').createOrReplaceTempView("tmp_1")

        sql_context.sql(
            '''
            select id, collect_set(item) links from tmp_1 group by id
            ''').createOrReplaceTempView('t_links')

        sql_context.sql(
            '''
            select t1.id, explode(array(t1.link.source_id, t1.link.target_id)) as node
            from (select id, explode(paths) as link from v_paths_with_attr) t1
            ''').distinct().createOrReplaceTempView('v_nodes')

        sql_context.sql(
            '''
            select
                t1.id,
                named_struct(
                    "node_id", t2.id,
                    "eid", t2.qixinbao_id,
                    "node_type", t2.entity_type,
                    "node_name", t2.entity_name) as node from
            (select id, node from v_nodes) t1
            left outer join
            (select id, qixinbao_id, entity_type, entity_name from guarantee_tag_table) t2
            on t1.node = t2.id
            where t2.id is not null
            ''').createOrReplaceTempView('v_nodes_tmp')

        sql_context.sql('select id, collect_set(node) nodes from v_nodes_tmp group by id') \
            .createOrReplaceTempView('t_nodes')

        sql_context.sql(
            '''
            select t1.id, t1.links, t2.nodes
            from
                (select id, links from t_links) t1
            left outer join
                (select id, nodes from t_nodes) t2
            on t1.id = t2.id
            where t1.id is not null and t2.id is not null
            ''').createOrReplaceTempView('t_result')

        df_result = sql_context.sql(
            """
            select t2.qixinbao_id as id,
                named_struct("links", t1.links, "nodes", t1.nodes) as relation_list
            from t_result t1
            left outer join guarantee_tag_table t2
            on t1.id = t2.id
            where t2.qixinbao_id is not null and t2.qixinbao_id != ''""").distinct()

        result_rdd = df_result.toJSON()
        format_rdd = sql_context.createDataFrame(result_rdd.map(lambda a: Row(json.loads(a)['id'], map_string(a)))) \
            .toDF('id', 'value')

        format_rdd.repartition(800).write.mode('overwrite').saveAsTable(output_table)

        os.system('hive -e "INSERT OVERWRITE TABLE test.grnt1 SELECT id, value FROM output_table;"')

        del components
    except Exception as ex:
        print(ex)
