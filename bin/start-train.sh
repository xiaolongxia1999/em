#!/bin/bash

#注意，部署的时候：1—用dos2unix命令将BOM符合去掉，2—添加+x权限

project_dir="$(cd "`dirname "$0"`"/..;pwd)"
conf_dir=${project_dir}/conf

#$1 the first command arg,should be a file of an existed hdfs file system path,like 'hdfs://master:port/yourPath' or 'yourPath'
#$2 the second command arg ,should be like "*.json"


#echo "nohup spark-submit --class $1  --master spark://172.16.32.139:7077 --total-executor-cores 20 --driver-memory 4g --executor-memory 4g --conf "spark.sql.pivotMaxValues=100000" --conf "spark.shuffle.consolidateFiles=true" --conf "spark.default.parallelism=48"   --conf "spark.executor.extraJavaOptions=-XX:Permsize=32m -Xmn512m -XX:+UseParallelGC"  ${project_dir}/assemblyJar/energymanagement.jar $2 ${conf_dir}/$3  ${project_dir}/data/tmp/te/temp/$4  &"

if [ $# -lt 4 ]; then
    echo "You should input 4 args !"
    echo "Usage: bin/start-predict YourMainClassWithPackageName YourDataPath YourJsonPath YourModelSavePath"
else
    nohup spark-submit --class $1  --master spark://172.16.32.139:7077 --total-executor-cores 20 --driver-memory 4g --executor-memory 4g --conf "spark.sql.pivotMaxValues=100000" --conf "spark.shuffle.consolidateFiles=true" --conf "spark.default.parallelism=48"   --conf "spark.executor.extraJavaOptions=-XX:PermSize=32m -Xmn512m -XX:+UseParallelGC"  ${project_dir}/assemblyJar/energymanagement.jar $2 ${conf_dir}/$3  ${project_dir}/data/tmp/$4 &
fi





