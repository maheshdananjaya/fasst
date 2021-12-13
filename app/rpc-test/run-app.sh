#!/usr/bin/env bash

#source run.sh
source ./hosts.sh
sudo killall memcached
echo "Node Id - ${NODE_ID}"
bash run-servers.sh ${NODE_ID}
