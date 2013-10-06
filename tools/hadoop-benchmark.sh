PSSH=parallel-ssh
NAMENODE=128.84.227.101
PORT=54310
WTF_DIR=${HOME}/src/wtf
DATANODES=${WTF_DIR}/.hadoop_data_hosts
CLIENTS=${WTF_DIR}/.hadoop_client_hosts
OUTPUT_DIR=${HOME}/hadoop

kill_cluster()
{
    echo "KILLING CLUSTER...\n"
    ${PSSH} -H ${NAMENODE} -i stop-dfs.sh
}

clean_cluster()
{
    echo "CLEANING CLUSTER...\n"
    ${PSSH} -H ${NAMENODE} -h ${DATANODES} -i "rm -rf ${OUTPUT_DIR} && \
        mkdir -p ${OUTPUT_DIR}/tmp/sean/dfs/name && mkdir ${OUTPUT_DIR}/tmp/sean/dfs/data"
}

reset_cluster()
{
    kill_cluster
    clean_cluster
    echo "FORMATTING NAMENODE AND STARTING DFS...\n"
    ssh ${NAMENODE} "hadoop namenode -format -force && start-dfs.sh"
    sleep 10
}

run_iteration()
{
    echo "RUNNING ITERATION...\n"
    ${PSSH} -p 100 -t 6000 -h ${CLIENTS}  -i \
        ${WTF_DIR}/$1 -h ${NAMENODE} -p ${PORT} \
        --file-length='$((64-PSSH_NODENUM))' --file-charset="hex" \
        --value-length=$2 -n $3 --output=$1'$((PSSH_NODENUM))'.log
}
reset_cluster
run_iteration wtf-hadoop-benchmark 67108864 100
kill_cluster
