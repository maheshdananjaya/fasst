#!/usr/bin/env bash
# Set LAT_WORKER to -1 to disable latency measurement or to worker id (i.e., from 0 up to [num-worker - 1])
LAT_WORKER="-1"
#LAT_WORKER="0"
APP=rpc-test
EXEC_FOLDER="${HOME}/fasst/app/${APP}"
REMOTE_COMMAND="cd ${EXEC_FOLDER}; bash run-app.sh"
cd ${EXEC_FOLDER}

# get Hosts
source ./hosts.sh
echo ${REMOTE_HOSTS}
#../bin/copy-exec-files.sh
bash run-app.sh
sleep 2 # give some leeway so that manager starts before executing the members
parallel "echo running | ssh -tt {} $'${REMOTE_COMMAND}'" ::: $(echo ${REMOTE_HOSTS[@]}) >/dev/null
cd - >/dev/null

#../bin/get-system-xput-files.sh
