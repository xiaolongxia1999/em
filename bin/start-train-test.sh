project_dir="$(cd "`dirname "$0"`"/..;pwd)"

nohup ${project_dir}/bin/start-train-teModel.sh /usr/cl/transferEntropy/data/yangsDataNoTranspose.csv te_json.txt yangsResult10 &
