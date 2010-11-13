#!/bin/bash

function root() {
    cmd=$1

    sudo sh -c "$1"
    # sh -c "$1"
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

restore_cpus
