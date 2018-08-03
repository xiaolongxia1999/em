#!/bin/bash
project_dir="$(cd "`dirname "$0"`"/..;pwd)"
conf_dir=${project_dir}/conf
#$1 the first command arg,should be a file of an existed hdfs file system path,like 'hdfs://master:port/yourPath/yourFilename.csv' or 'yourPath'
#$2 the second command arg ,should be like "*.json"
#$3 the third command arg,should be like a integer, like "1"

nohup spark-submit --class "com.bonc.examples.TransferEntropy1"   --total-executor-cores 20 --driver-memory 4g --executor-memory 4g --conf "spark.sql.pivotMaxValues=100000" --conf "spark.shuffle.consolidateFiles=true" --conf "spark.default.parallelism=48"   --conf "spark.executor.extraJavaOptions=-XX:PermSize=32m -Xmn512m -XX:+UseParallelGC"  ${project_dir}/assemblyJar/energymanagement.jar $1 ${conf_dir}/$2  ${project_dir}/data/tmp/ $3 &

#example Usage:
#bin/start-train-teModel.sh /usr/cl/transferEntropy/data/yangsDataNoTranspose.csv te_json.txt yangsResult1
