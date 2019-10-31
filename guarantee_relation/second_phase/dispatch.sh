#!/usr/bin/env bash

# Airflow config

# First, guarantee data ETL.
sh grnt_etl.sh

# Then, process graph. Depart to subprocess: one is cutoff the relation level, another is total relation.
sh grnt_graph_queue.sh

# Last, read json file and write to hive.
sh json_file_to_hive.sh