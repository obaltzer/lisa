#!/bin/bash
#
#PBS -S /bin/bash
#PBS -l nodes=1:ppn=8
#PBS -l walltime=8:00:00
#PBS -N query1_scalability
#PBS -t 1-10

if [ "$PBS_JOBNAME" ] ; then
  JOBNAME=$PBS_JOBNAME
else
  JOBNAME=testing
fi

LISA_HOME=$HOME/phd/lisa
RESULTS_DIR=${HOME}/phd/results/lisapy/${JOBNAME}
DATA_HOME=$HOME/phd/data
SOURCE_DIR=${DATA_HOME}/synthetic/non-spatial/specs
SCRATCH_DIR=/state/partition1/obaltzer/phd/data/lisapy
INDEX_TOOL=${LISA_HOME}/tools/create_index.py
GENDATA_TOOL="bash ${DATA_HOME}/scripts/gendata.sh"

TRACKS="1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16"
#TRACKS="11"
QUERY_SCRIPT=${LISA_HOME}/experiments/query1.py
RUNNING=1

# Determine the dataset that is to be used.
if [ "${PBS_ARRAYID}" ] ; then
  SIZE="${PBS_ARRAYID}00000"
else
  SIZE=100000
fi

INPUT_FILES="plants_A_${SIZE}.conf"

N_CPU=8

mkdir -p ${RESULTS_DIR}
if [ "${PBS_JOBID}" ] ; then
    filename=$(echo ${PBS_JOBID} | cut -d. -f1)
    RESULTS_FILE=${RESULTS_DIR}/${filename}_${SIZE}.csv
else
    RESULTS_FILE=${RESULTS_DIR}/results.csv
fi
touch ${RESULTS_FILE}


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
    for f in ${INPUT_FILES} ; do
        local source=${SOURCE_DIR}/$f
        local dest=${SCRATCH_DIR}/${f/%.conf/.db}
        local destpath=$(dirname ${dest})
    
        if [ ! -f ${dest} -o ${source} -nt ${dest} ] ; then
            echo "Creating dataset ${dest}"
            pushd ${destpath}
            ${GENDATA_TOOL} ${source}
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
    for f in ${INPUT_FILES} ; do
        args="${args} ${SCRATCH_DIR}/${f/%.conf/.db}"
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
    for f in ${INPUT_FILES} ; do
        args="${args} ${SCRATCH_DIR}/${f/%.conf/.db}"
    done
    if [ ${RUNNING} -eq 1 ] ; then
        pushd ${SCRATCH_DIR}
        cmd="python ${QUERY_SCRIPT} ${tracks} ${args}"
        echo $cmd
        local output=$(timed ${cmd})
        local result="${N_CPU},${tracks},${SIZE},${output}"
        echo ${result} >> ${RESULTS_FILE}
        popd
    fi
}

trap control_c SIGINT

# convert the input
convert_input

# Configure CPUs
configure_cpus

# warm up the cache
run 8

# run experiments
for t in ${TRACKS} ; do
    run_timed ${t}
done

# restore CPUs
restore_cpus
