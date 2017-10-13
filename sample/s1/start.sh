#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21

DEPLOY_MODE=client
LOCAL_ROOT_DIR=/Users/deanzhang/work/code/github/lego-framework/sample/s1
SPARK_PARAMTERS="--conf spark.shuffle.consolidateFiles=true --executor-memory 1500m --conf spark.rdd.compress=true"
HDFS_MODELS_ROOT_DIR=hdfs://hadoop1:8020/models/
./submit_job.sh $DEPLOY_MODE $LOCAL_ROOT_DIR "$SPARK_PARAMTERS" $HDFS_MODELS_ROOT_DIR
