#!/usr/bin/env bash
date=$(date +%Y%m%d)
touch /opt/danbaoguanxi/logs/grnt_graph_queue.log.${date}
python grnt_graph_queue.py 10 >> /opt/danbaoguanxi/logs/grnt_graph_queue.log.${date}

if [ "$?" != "0" ]; then
    echo "Failed to execute grnt_graph_queue.py at `date '+%Y-%m-%d %H:%M:%S'`"
    exit 1
else
    echo "Succeeded to execute grnt_graph_queue.py at `date '+%Y-%m-%d %H:%M:%S'`"
fi