#!/usr/bin/env bash

ssh -i /xxx/mmy_id_rsa user@xxx << remotessh

file_path=''
hdfs_path=''
neo4j_dir=''
database=''
username=''
password=''
bolt=''

nodes_guarantee=${file_path}'/nodes_guarantee.csv'
nodes_corp=${file_path}'/nodes_corp.csv'
edges_guarantee=${file_path}'/edges_guarantee.csv'
edges_belong=${file_path}'/edges_belong.csv'
edges_corp=${file_path}'/edges_corp.csv'
edges_corp_agg=${file_path}'/edges_corp_agg.csv'

nodes_guarantee_header=${file_path}'/headers/nodes_guarantee-header.csv'
nodes_corp_header=${file_path}'/headers/nodes_corp-header.csv'
edges_guarantee_header=${file_path}'/headers/edges_guarantee-header.csv'
edges_belong_header=${file_path}'/headers/edges_belong-header.csv'
edges_corp_header=${file_path}'/headers/edges_corp-header.csv'
edges_corp_agg_header=${file_path}'/headers/edges_corp_agg-header.csv'

cd ${file_path}

rm -f ${guarantee_nodes_file_path}
rm -f ${corp_nodes_file_path}
rm -f ${guarantee_relation_file_path}
rm -f ${belong_relation_file_path}
rm -f ${corp_relation_file_path}
rm -f ${corp_agg_relation_file_path}

hdfs dfs -getmerge ${hdfs_path}'/nodes_guarantee' ${guarantee_nodes_file_path}
hdfs dfs -getmerge ${hdfs_path}'/nodes_corp'      ${corp_nodes_file_path}
hdfs dfs -getmerge ${hdfs_path}'/edges_guarantee' ${guarantee_relation_file_path}
hdfs dfs -getmerge ${hdfs_path}'/edges_belong'    ${belong_relation_file_path}
hdfs dfs -getmerge ${hdfs_path}'/edges_corp'      ${corp_relation_file_path}
hdfs dfs -getmerge ${hdfs_path}'/edges_corp_agg'  ${corp_agg_relation_file_path}

cd ${neo4j_dir}
./bin/neo4j stop

rm -rf ./data/

./bin/neo4j-admin import
  --mode=csv --database=${database}
  --nodes:Guarantee ${nodes_guarantee_header},${nodes_guarantee}
  --nodes:Corp ${nodes_corp_header},${nodes_corp}
  --relationships:GuaranteeRel ${edges_guarantee_header},${edges_guarantee}
  --relationships:BelongRel ${edges_belong_header},${edges_belong}
  --relationships:CorpRel ${edges_corp_header},${edges_corp}
  --relationships:CorpAggRel ${edges_corp_agg_header},${edges_corp_agg}
  --ignore-extra-columns true
  --ignore-duplicate-nodes true
  --ignore-missing-nodes true
  --delimiter "\001"
  --quote "'";

./bin/neo4j start

./bin/cypher-shell -a ${bolt} -u ${username} -p neo4j 'CALL dbms.changePassword("'${password}'")' ;

./bin/cypher-shell -a ${bolt} -u ${username} -p ${password} 'CREATE INDEX ON :Guarantee(nodeId);';
./bin/cypher-shell -a ${bolt} -u ${username} -p ${password} 'CREATE INDEX ON :Guarantee(eid);';
./bin/cypher-shell -a ${bolt} -u ${username} -p ${password} 'CREATE INDEX ON :Guarantee(corpId);';
./bin/cypher-shell -a ${bolt} -u ${username} -p ${password} 'CREATE INDEX ON :Corp(corpId);';

./bin/cypher-shell -a ${bolt} -u ${username} -p ${password} 'MATCH (n) OPTIONAL MATCH (n)-[r]->() RETURN count(n.prop) + count(r.prop)';

exit
remotessh