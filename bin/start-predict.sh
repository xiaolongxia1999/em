#!/bin/bash
project_dir="$(cd "`dirname "$0"`"/..;pwd)"
conf_dir=${project_dir}/conf

#$1 the first command arg,should be a file of an existed hdfs file system path,like 'hdfs://master:port/yourPath/yourFilename.csv' or 'yourPath'
#$2 the second command arg ,should be like "*.json"
#$3 the third command arg,should be like a integer, like "1"

if [ $# -lt 4 ]; then
    echo "You should input 3 args !"
    echo "Usage: bin/start-predict YourMainClassWithPackageName YourDataPath YourJsonPath YourModelSavePath"
else
    spark-submit --class $1   --total-executor-cores 20 --driver-memory 4g --executor-memory 4g --conf "spark.sql.pivotMaxValues=100000" --conf "spark.shuffle.consolidateFiles=true" --conf "spark.default.parallelism=48"   --conf "spark.executor.extraJavaOptions=-XX:PermSize=32m -Xmn512m -XX:+UseParallelGC"  ${project_dir}/assemblyJar/energymanagement.jar $2 ${conf_dir}/$3  ${project_dir}/data/tmp/$4
fi



#example Usage:
#bin/start-train-teModel.sh /usr/cl/transferEntropy/data/yangsDataNoTranspose.csv te_json.txt yangsResult1