#!/bin/bash
#add by Dean at 2017-05-27

if [ "$1" = "--help" ]; then
	echo -e "submit_job.sh {deploy_mode(client/cluster)} {local_root_dir} {spark_parameters} {hdfs_models_root_dir}\n for client: ./submit_job.sh client /home/sa/app/models/v3.0.0/client/pred_hourly \"--executor-memory 4G --conf spark.app.name=pred_hourly --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true\"\n for cluster: ./submit_job.sh cluster /home/sa/app/models/v3.0.0/client/pred_hourly \"--executor-memory 4G --conf spark.app.name=pred_hourly --conf spark.shuffle.consolidateFiles=true --conf spark.rdd.compress=true\" hdfs://ns1/work/bw/bi/models"
	exit 0
fi

# get deploy mode by input parameter
DEFAULT_DEPLOY_MODE=client
# client or cluster or local
INPUT_DEPLOY_MODE=$1
DEPLOY_MODE=${INPUT_DEPLOY_MODE:-$DEFAULT_DEPLOY_MODE}

# set base parameters
ROOT_DIR=$2
echo "ROOT_DIR = $ROOT_DIR"
SPARK_PARAMETERS=$3
# must start with "hdfs://"
HDFS_MODELS_ROOT_DIR=$4
MODEL_NAME=$(echo $ROOT_DIR | awk -F '/' '{print $NF}')
echo "MODEL_NAME = $MODEL_NAME"
HDFS_ROOT_DIR="$HDFS_MODELS_ROOT_DIR/$MODEL_NAME"
echo "HDFS_ROOT_DIR = $HDFS_ROOT_DIR"
LEGO_CORE_JAR_KEY="lego-core-assembly-*.jar"
#get framework jar file name
cd $ROOT_DIR
LEGO_CORE_JAR=$(ls $LEGO_CORE_JAR_KEY)
#process cluster mode
if [ "$DEPLOY_MODE" = "cluster" ]; then
	CONF_LIST=$(find $ROOT_DIR -type f -name "application.conf")
	#remove models dir on hdfs if it existed
	hadoop fs -test -e $HDFS_ROOT_DIR
	if [ $? -eq 0 ];then
		echo "$HDFS_ROOT_DIR exsited, remove it"
		hadoop fs -rm -r -skipTrash $HDFS_ROOT_DIR
	fi
	#upload local application.conf to hdfs
	for c in ${CONF_LIST[*]}
	do
		LOCAL_FILE=$c
		HDFS_FILE="$HDFS_ROOT_DIR/$(echo $LOCAL_FILE | awk -F "$MODEL_NAME/" '{print $NF}')"
		HDFS_DIR=`dirname $HDFS_FILE`
		NOT_MODULE=$(cat $LOCAL_FILE | grep -iE 'type.*=.*"application"|type.*=.*"system"' | wc -l)
		if [ $NOT_MODULE -gt 0 ]; then
			sed -i.bak -e "s#$ROOT_DIR#$HDFS_ROOT_DIR#g" $LOCAL_FILE
		fi
		hadoop fs -mkdir -p $HDFS_DIR
		hadoop fs -put $LOCAL_FILE $HDFS_FILE
		if [ $NOT_MODULE -gt 0 ]; then
			mv "$LOCAL_FILE.bak" $LOCAL_FILE
		fi
		echo "Finished upload $HDFS_FILE"
	done

	#upload lego-core jar file to hdfs
	#What can be optimized：
	#add a parameter to decide if upload lego-core jar file every time or not
	hadoop fs -put "$ROOT_DIR/$LEGO_CORE_JAR" $HDFS_ROOT_DIR
	echo "Finished upload $HDFS_ROOT_DIR/$LEGO_CORE_JAR"
fi

#start submit application to yarn
SUBMIT_DIR=$ROOT_DIR
if [ "$DEPLOY_MODE" = "cluster" ]; then
	SUBMIT_DIR=$HDFS_ROOT_DIR
fi

MASTER_PARAM=""
if [ "$DEPLOY_MODE" = "cluster" ]; then
	MASTER_PARAM="--master yarn --deploy-mode cluster"
elif [ "$DEPLOY_MODE" = "client" ]; then
    MASTER_PARAM="--master yarn --deploy-mode client"
else
    MASTER_PARAM="--master local"
fi

echo "MASTER_PARAM = $MASTER_PARAM"
spark-submit --conf spark.app.name=$MODEL_NAME --jars $(find $ROOT_DIR -type f -name "*.jar" -not -name "lego-core*" | awk -F/ '{ $0=$0 ; print $NF","$0}' | sort -u -t, -k1,1 | cut -d"," -f2 | xargs echo | tr ' ' ',') --class cn.dean.lego.core.Launcher $MASTER_PARAM $SPARK_PARAMETERS "$SUBMIT_DIR/$LEGO_CORE_JAR" $SUBMIT_DIR/conf/application.conf