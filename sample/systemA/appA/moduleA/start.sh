#!/bin/bash
#add by hadoop at 2016-01-21 17:32:21 

ROOT_DIR=/Users/deanzhang/work/code/pms/rms-1st/lego-framework/sample/systemA/appB/moduleA

spark-submit --jars $(find ${ROOT_DIR} -type f -name "*.jar" -not -name "lego-core*" | xargs echo | tr ' ' ',') --class cn.jw.rms.data.framework.core.Main --master local[*] --driver-memory 4G --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true ${ROOT_DIR}/lego-core-assembly-2.0.jar ${ROOT_DIR}/conf/application.conf
