#!/bin/bash
#
#PBS -S /bin/bash
#PBS -l nodes=1:ppn=8
#PBS -l walltime=48:00:00
#PBS -N query8
#PBS -t 1-8

if [ "$PBS_JOBNAME" ] ; then
  JOBNAME=$PBS_JOBNAME
else
  JOBNAME=testing
fi

LISA_HOME=$HOME/phd/lisa
RESULTS_DIR=${HOME}/phd/results/lisapy/${JOBNAME}
DATA_HOME=$HOME/phd/data
SOURCE_DIR=${DATA_HOME}/synthetic/tpch
SCRATCH_DIR=/state/partition1/obaltzer/phd/data/lisapy
INDEX_TOOL=${LISA_HOME}/tools/create_index.py
TPCH_SQLITE=${LISA_HOME}/tools/tpch_sqlite.sh
GENDATA_TOOL="bash ${DATA_HOME}/scripts/gendata.sh"

SF=50
TRACKS="1 2 3 4 5 6 7 8 9 10 11 12 13 14"
#TRACKS="11"
QUERY_SCRIPT=${LISA_HOME}/experiments/query8.py
RUNNING=1

mkdir -p ${RESULTS_DIR}
if [ "${PBS_JOBID}" ] ; then
    filename=$(echo ${PBS_JOBID} | cut -d. -f1)
    RESULTS_FILE=${RESULTS_DIR}/${filename}.csv
else
    RESULTS_FILE=${RESULTS_DIR}/results.csv
fi
touch ${RESULTS_FILE}

if [ "${PBS_ARRAYID}" ] ; then
    N_CPU=${PBS_ARRAYID}
else
    N_CPU=8
fi

. $LISA_HOME/bin/lisaenv

###################################################
#
# Utility functions
#
###################################################
function root() {
    cmd=$1

    sudo sh -c "$1"
    # sh -c "$1"
}

function control_c() {
    RUNNING=0
    echo "Ctrl+C was pressed."
    kill -KILL -$$
    exit $?
}

function timed() {
    {
        /usr/bin/time -f "%e,%U,%S,%P" $@ >/dev/null
    } 2>&1 | {
        local n=0
        local out=""
        while read l ; do
            if [ "${n}" -eq 0 ] ; then
                out=${l}
            else
                out="${out},${l}"
            fi
            n=$((n+1))
        done
        echo ${out}
    }
}

###################################################
#
# Convert input files
#
###################################################
function convert_input() {
    mkdir -p ${SCRATCH_DIR}
    for sf in ${SF} ; do
      if [ ! -f ${SCRATCH_DIR}/tpch_${sf}.db ] ; then
        for f in region nation customer orders lineitem ; do
          cp -v ${SOURCE_DIR}/${f}_${sf}.txt ${SCRATCH_DIR}
        done
        pushd ${SCRATCH_DIR}
        ${TPCH_SQLITE} ${sf} all
        popd
      fi
    done
}

###################################################
#
# Configure CPUs
#
###################################################
function configure_cpus() {
    local cpus=/sys/devices/system/cpu/cpu*
    local maxcpu=$(($N_CPU - 1))
    for p in ${cpus} ; do
        local index=$(basename ${p})
        index=${index:3:4}
        if [ -f ${p}/online ] ; then
            if [ ${index} -le ${maxcpu} ] ; then
                # p <= N_CPU so turn this CPU on
                echo "Turning CPU${index} on"
                [ $(root "cat ${p}/online") -eq 0 ] \
                    && root "echo 1 > ${p}/online"
            else
                echo "Turning CPU${index} off"
                [ $(root "cat ${p}/online") -eq 1 ] \
                    && root "echo 0 > ${p}/online"
            fi
        fi
    done
}

function restore_cpus() {
    local cpus=/sys/devices/system/cpu/cpu*
    for p in ${cpus} ; do
        if [ -f ${p}/online ] ; then
            echo "Turning CPU ${p} on"
            [ $(root "cat ${p}/online") -eq 0 ] \
                && root "echo 1 > ${p}/online"
        fi
    done
}
    

###################################################
#
# Run experiment
#
###################################################
function run() {
    local tracks=$1
    local args=""
    for sf in ${SF} ; do
        args="${args} ${SCRATCH_DIR}/tpch_${sf}.db"
    done
    if [ ${RUNNING} -eq 1 ] ; then
        pushd ${SCRATCH_DIR}
        cmd="python ${QUERY_SCRIPT} ${tracks} ${args}"
        echo $cmd
        $cmd > /dev/null 2>&1 
        popd
    fi
}

function run_timed() {
    local tracks=$1
    local args=""
    for sf in ${SF} ; do
        args="${args} ${SCRATCH_DIR}/tpch_${sf}.db"
    done
    if [ ${RUNNING} -eq 1 ] ; then
        pushd ${SCRATCH_DIR}
        cmd="python ${QUERY_SCRIPT} ${tracks} ${args}"
        echo $cmd
        local output=$(timed ${cmd})
        local result="${N_CPU},${tracks},${output}"
        echo ${result} >> ${RESULTS_FILE}
        popd
    fi
}

trap control_c SIGINT

# convert the input
convert_input

# Configure CPUs
sudo /share/apps/admin/setcpu $N_CPU

# warm up the cache
run 8

# run experiments
for t in ${TRACKS} ; do
    run_timed ${t}
done

# restore CPUs
sudo /share/apps/admin/setcpu
