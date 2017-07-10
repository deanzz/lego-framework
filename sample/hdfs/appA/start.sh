#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21

ROOT_DIR=hdfs://hadoop1:8020/models/appA

spark-submit --class cn.jw.rms.data.framework.core.Main --master yarn-cluster --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/lego-core-assembly-2.0.2.jar ${ROOT_DIR}/conf/application.conf