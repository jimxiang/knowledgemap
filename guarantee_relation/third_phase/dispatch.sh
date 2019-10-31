#!/usr/bin/env bash

# Task process:
# P1 --> P2.1 --> P2.2 --> P2.2
# P1 --> P3.1 --> P3.2
# P1, data materialize.
sh ./full_data/materialize.sh

if [[ "$?" != "0" ]]; then
    echo "Failed to execute materialize.sh at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute materialize.sh at `date '+%Y-%m-%d %H:%M:%S'`"
fi

# P2.1, data ETL.
sh ./full_data/grnt_etl.sh

if [[ "$?" != "0" ]]; then
    echo "Failed to execute grnt_etl.sh at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute grnt_etl.sh at `date '+%Y-%m-%d %H:%M:%S'`"
fi

# P2.2, process graph.
sh ./full_data/grnt_graph_queue.sh

if [[ "$?" != "0" ]]; then
    echo "Failed to execute grnt_graph_queue.sh at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute grnt_graph_queue.sh at `date '+%Y-%m-%d %H:%M:%S'`"
fi

# P2.3, write to hbase.
sh ./full_data/json_file_to_hive.sh

if [[ "$?" != "0" ]]; then
    echo "Failed to execute json_file_to_hive.sh at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute json_file_to_hive.sh at `date '+%Y-%m-%d %H:%M:%S'`"
fi

# P3.1, prepare neo4j data.
sh ./neo4j/prepare_neo4j_data.sh

if [[ "$?" != "0" ]]; then
    echo "Failed to execute prepare_neo4j_data.sh at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute prepare_neo4j_data.sh at `date '+%Y-%m-%d %H:%M:%S'`"
fi

# P3.2, import to neo4j.
sh ./neo4j/neo4j_import.sh

if [[ "$?" != "0" ]]; then
    echo "Failed to execute neo4j_import.sh at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute neo4j_import.sh at `date '+%Y-%m-%d %H:%M:%S'`"
fi