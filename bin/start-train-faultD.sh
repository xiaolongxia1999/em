#!/bin/bash

project_dir="$(cd "`dirname "$0"`"/..;pwd)"

modelName="com.bonc.examples.FaultDObject"
nohup ${project_dir}/bin/start-train.sh ${modelName} $1 $2 $3 &

if [ $# -lt 3 ]; then
    echo "You should input 3 args !"
    echo "Usage: bin/start-train.sh YourDataPath YourJsonPath YourModelSavePath"
else
    nohup ${project_dir}/bin/start-train.sh ${modelName} $1 $2 $3 &
fi