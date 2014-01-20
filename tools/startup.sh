WTF_DIR=/home/sean/src/wtf
WTF_COORDINATOR=${WTF_DIR}/.wtf_coordinator_host
WTF_PORT=1981
HYPERDEX_COORDINATOR=${WTF_DIR}/.hyperdex_coordinator_host
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

ALL_HOSTS=`mktemp`
sort .wtf_client_hosts .wtf_daemon_hosts .hyperdex_daemon_hosts .wtf_coordinator_host .hyperdex_coordinator_host | sort -u > ${ALL_HOSTS}

HYPERDEX_HOSTS=`mktemp`
sort .hyperdex_daemon_hosts .hyperdex_coordinator_host | sort -u > ${HYPERDEX_HOSTS}

WTF_HOSTS=`mktemp`
sort .wtf_daemon_hosts .wtf_coordinator_host | sort -u > ${WTF_HOSTS}

CLUSTER_HOSTS=`mktemp`
sort .wtf_daemon_hosts .hyperdex_daemon_hosts .wtf_coordinator_host .hyperdex_coordinator_host | sort -u > ${CLUSTER_HOSTS}

HC=`cat ${HYPERDEX_COORDINATOR}`
WC=`cat ${WTF_COORDINATOR}`



kill_cluster()
{
    echo "KILLING CLUSTER...\n"
    pkill -9 hyper
    pkill -9 replicant
    pkill -9 wtf 
}

clean_cluster()
{
    echo "CLEANING CLUSTER...\n"
    rm -rf ${DATA_DIR}
    mkdir -p ${WTF_COORDINATOR_DATA_DIR} \
        && mkdir ${WTF_DAEMON_DATA_DIR}
    mkdir ${HYPERDEX_COORDINATOR_DATA_DIR}
    mkdir ${HYPERDEX_DAEMON_DATA_DIR}
    rm wtf-*.log
    mkdir ${WTF_DAEMON_DATA_DIR}/data
}

reset_cluster()
{
    kill_cluster
    clean_cluster
    echo "STARTING HYPERDEX COORDINATOR...\n"
    ${HYPERDEX} coordinator -D ${HYPERDEX_COORDINATOR_DATA_DIR} -l ${HC} -p ${HYPERDEX_PORT}
    sleep 5
    echo "STARTING HYPERDEX DAEMONS...\n"
    ${HYPERDEX} daemon -D ${HYPERDEX_DAEMON_DATA_DIR} -c ${HC} -P ${HYPERDEX_PORT} -t 1
    sleep 5
    echo "ADDING WTF SPACE...\n"
    echo 'space wtf key path attributes map(string, string) blockmap, int directory, int mode' | ${HYPERDEX} add-space -h ${HC} -p ${HYPERDEX_PORT}
    sleep 5
    echo "STARTING WTF COORDINATOR...\n"
    ${WTF} coordinator -D ${WTF_COORDINATOR_DATA_DIR} -l ${WC} -p ${WTF_PORT} -d
    sleep 5
    echo "STARTING WTF DAEMONS...\n"
    ${WTF} daemon -D ${WTF_DAEMON_DATA_DIR} -M ${WTF_DAEMON_DATA_DIR}/data/metadata -c ${WC} -P ${WTF_PORT} -t 1 -f
    #libtool --mode=execute gdb --args ${WTF} daemon -D ${WTF_DAEMON_DATA_DIR} -M ${WTF_DAEMON_DATA_DIR}/data/metadata -c ${WC} -P ${WTF_PORT} -t 1 -f
    sleep 5
}
#
#run_iteration()
#{
#    echo "RUNNING ITERATION...\n"
#    ${PSSH} -p 100 -t 6000 -h ${WTF_CLIENTS}  -i \
#        ${WTF_DIR}/$1 -h ${WC} -p ${WTF_PORT} -H ${HC} -P ${HYPERDEX_PORT} \
#        --file-length=$((64-PSSH_NODENUM)) --file-charset='hex' \
#        --value-length=$2 -n $3 --output=$1'$((PSSH_NODENUM))'.log
#}

reset_cluster
#run_iteration wtf-sync-benchmark 512 10
#kill_cluster
