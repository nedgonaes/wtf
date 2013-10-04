PSSH=parallel-ssh
WTF_DIR=/home/sean/src/wtf
WTF_COORDINATOR=128.84.227.102
WTF_PORT=1982
HYPERDEX_COORDINATOR=128.84.227.101
HYPERDEX_PORT=1982
WTF_DAEMONS=${WTF_DIR}/.wtf_daemon_hosts
HYPERDEX_DAEMONS=${WTF_DIR}/.hyperdex_daemon_hosts
WTF_CLIENTS=${WTF_DIR}/.wtf_client_hosts
DATA_DIR=${WTF_DIR}/wtf-data
WTF_COORDINATOR_DATA_DIR=${DATA_DIR}/wtf-coordinator
WTF_DAEMON_DATA_DIR=${DATA_DIR}/wtf-daemon
HYPERDEX_DAEMON_DATA_DIR=${DATA_DIR}/hyperdex-daemon
HYPERDEX_COORDINATOR_DATA_DIR=${DATA_DIR}/hyperdex-coordinator
HYPERDEX=hyperdex
WTF=wtf

kill_cluster()
{
    echo "KILLING CLUSTER...\n"
    ${PSSH} -H ${HYPERDEX_COORDINATOR} -h ${HYPERDEX_DAEMONS} -i pkill -9 hyper
    ${PSSH} -H ${HYPERDEX_COORDINATOR} -h ${HYPERDEX_DAEMONS} -i pkill -9 replicant
    ${PSSH} -H ${WTF_COORDINATOR} -h ${WTF_DAEMONS} -h ${WTF_CLIENTS} -i pkill -9 wtf 
    ${PSSH} -H ${WTF_COORDINATOR} -h ${WTF_DAEMONS} -i pkill -9 replicant
}

clean_cluster()
{
    echo "CLEANING CLUSTER...\n"
    ${PSSH} -H ${WTF_COORDINATOR} -h ${WTF_DAEMONS} -h ${HYPERDEX_DAEMONS} -i "rm -rf ${DATA_DIR}; mkdir -p ${WTF_COORDINATOR_DATA_DIR} \
        && mkdir ${WTF_DAEMON_DATA_DIR} && mkdir ${HYPERDEX_COORDINATOR_DATA_DIR} \
        && mkdir ${HYPERDEX_DAEMON_DATA_DIR} && rm wtf-*.log"
}

reset_cluster()
{
    kill_cluster
    clean_cluster
    echo "STARTING HYPERDEX COORDINATOR...\n"
    ssh ${HYPERDEX_COORDINATOR} "${HYPERDEX} coordinator -D ${HYPERDEX_COORDINATOR_DATA_DIR} -l ${HYPERDEX_COORDINATOR} -p ${HYPERDEX_PORT}"
    sleep 5
    echo "STARTING HYPERDEX DAEMONS...\n"
    ${PSSH} -h ${HYPERDEX_DAEMONS} -i "${HYPERDEX} daemon -D ${HYPERDEX_DAEMON_DATA_DIR} -c ${HYPERDEX_COORDINATOR} -P ${HYPERDEX_PORT} -t 1"
    sleep 5
    echo "ADDING WTF SPACE...\n"
    ssh ${HYPERDEX_COORDINATOR} "echo 'space wtf key path attributes map(string, string) blockmap' | ${HYPERDEX} add-space -h ${HYPERDEX_COORDINATOR} -p ${HYPERDEX_PORT}"
    sleep 5
    echo "STARTING WTF COORDINATOR...\n"
    ssh ${WTF_COORDINATOR} "${WTF} coordinator -D ${WTF_COORDINATOR_DATA_DIR} -l ${WTF_COORDINATOR} -p ${WTF_PORT} -d"
    sleep 5
    echo "STARTING WTF DAEMONS...\n"
    ${PSSH} -h ${WTF_DAEMONS} -i "${WTF} daemon -D ${WTF_DAEMON_DATA_DIR} -c ${WTF_COORDINATOR} -P ${WTF_PORT} -t 1 -d"
    sleep 5
}

run_iteration()
{
    echo "RUNNING ITERATION...\n"
    ${PSSH} -p 100 -t 6000 -h ${WTF_CLIENTS}  -i \
        ${WTF_DIR}/$1 -h ${WTF_COORDINATOR} -p ${WTF_PORT} -H ${HYPERDEX_COORDINATOR} -P ${HYPERDEX_PORT} \
        --file-length=64 --file-alphabet='abcdefghijklmnopqrstuvwxyz0123456789' \
        --value-length=$2 -n $3 --output=$1'$((PSSH_NODENUM))'.log
}
reset_cluster
run_iteration wtf-sync-benchmark 67108864 100
kill_cluster
