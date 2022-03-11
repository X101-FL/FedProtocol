#! /bin/bash

WORK_SPACE=$(readlink -f "$(dirname $0)/../")
cd $WORK_SPACE

nohup python fedprototype/envs/cluster/spark/spark_coordinater_server.py \
    --host 127.0.0.1 \
    --port 6609 \
    > logs/spark_coordinater_server.log 2>&1 &
