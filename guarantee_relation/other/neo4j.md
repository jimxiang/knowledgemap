### Load Data From CSV
```
CREATE INDEX ON :GRNT(node_id);

load csv with headers from 'file:///graph_nodes.csv' as row merge (n:GRNT {node_id: row.node_id}) 
set n.node_type = row.node_type, n.node_name = row.node_name, n.eid = row.eid, n.corp_name = row.corp_name, n.corp_id = row.corp_id;

load csv with headers from 'file:///graph_edges.csv' as row 
merge (from:GRNT {node_id: row.grnt_custid}) 
merge (to:GRNT {node_id: row.bor_custid}) 
merge (from)-[rel:GRNT_REL]->(to) 
set rel.grnt_custid = row.grnt_custid, rel.bor_custid = row.bor_custid, rel.dw_stat_dt = row.dw_stat_dt, 
rel.grnt_cod = row.grnt_cod, rel.grnt_nm = row.grnt_nm, rel.bor_cod = row.bor_cod, rel.bor_nm = row.bor_nm, 
rel.grnt_ctr_id = row.grnt_ctr_id, rel.guaranteetype = row.guaranteetype, rel.currency = row.currency, 
rel.guaranteeamount = row.guaranteeamount, rel.sgn_dt = row.sgn_dt, rel.data_src = row.data_src, 
rel.cnv_cny_guaranteeamount = row.cnv_cny_guaranteeamount, rel.cnv_cny_exr = row.cnv_cny_exr, 
rel.circle_id = row.circle_id, rel.rel_type = row.rel_type

```

### APOC Path
```
match (grnt:GUARANTEE) where grnt.eid = '' call apoc.path.expand(grnt, 'GUARANTEE_REL', '', 0, 3) yield path return path
```

### APOC Load CSV
这种方式报错严重
```
call apoc.periodic.iterate('call apoc.load.csv("graph_nodes.csv") yield map as row return row', 
'MERGE (n:GRNT) SET n.node_type = row.node_type, n.node_name = row.node_name, n.eid = row.eid, n.corp_name = row.corp_name, n.corp_id = row.corp_id', 
{batchSize:10000, iterateList:true, parallel:true});

call apoc.periodic.iterate('call apoc.load.csv("graph_edges.csv") yield map as row return row', 
'merge (from:GRNT {node_id: row.grnt_custid}) 
merge (to:GRNT {node_id: row.bor_custid}) 
merge (from)-[rel:GRNT_REL]->(to) 
set rel.grnt_custid = row.grnt_custid, rel.bor_custid = row.bor_custid, rel.dw_stat_dt = row.dw_stat_dt, 
rel.grnt_cod = row.grnt_cod, rel.grnt_nm = row.grnt_nm, rel.bor_cod = row.bor_cod, rel.bor_nm = row.bor_nm, 
rel.grnt_ctr_id = row.grnt_ctr_id, rel.guaranteetype = row.guaranteetype, rel.currency = row.currency, 
rel.guaranteeamount = row.guaranteeamount, rel.sgn_dt = row.sgn_dt, rel.data_src = row.data_src, 
rel.cnv_cny_guaranteeamount = row.cnv_cny_guaranteeamount, rel.cnv_cny_exr = row.cnv_cny_exr, 
rel.circle_id = row.circle_id, rel.rel_type = row.rel_type', 
{batchSize:10000, iterateList:true, parallel:true});
```

### CSV Headers
```
Nodes:
node_id,eid,node_name,corp_name,corp_id,node_type

Edges:
dw_stat_dt,grnt_cod,grnt_custid,grnt_nm,bor_cod,bor_custid,bor_nm,grnt_ctr_id,guaranteetype,currency,guaranteeamount,sgn_dt,data_src,cnv_cny_guaranteeamount,cnv_cny_exr,circle_id,rel_type
```