#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21

ROOT_DIR=/home/sa/app/models/appB
HDFS_DIR=hdfs://hadoop1:8020/models/appB
spark-submit --jars $(find ${ROOT_DIR} -type f -name "*.jar" -not -name "lego-core*" | xargs echo | tr ' ' ',') --class cn.jw.rms.data.framework.core.Main --master yarn-client --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${HDFS_DIR}/lego-core-assembly-2.0.3.jar ${HDFS_DIR}/conf/application.conf
