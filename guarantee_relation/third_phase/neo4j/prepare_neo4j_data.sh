#!/usr/bin/env bash
#!/usr/bin/env bash
/opt/spark_cmds/spark2.1_submit.sh --name prepare_neo4j_data --executor-memory 20G --driver-memory 4g --executor-cores 2 --conf spark.dynamicAllocation.minExecutors=3 --conf spark.dynamicAllocation.maxExecutors=10 --conf spark.yarn.executor.memoryOverhead=20G --conf spark.dynamicAllocation.enabled=true --conf spark.shuffle.service.enabled=true --conf spark.driver.maxResultSize=8g /opt/danbaoguanxi/third/neo4j/prepare_neo4j_data.py

if [[ "$?" != "0" ]]; then
    echo "Failed to execute prepare_neo4j_data.sh at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute prepare_neo4j_data.sh at `date '+%Y-%m-%d %H:%M:%S'`"
fi