#!/usr/bin/env bash

#source run.sh
source ./hosts.sh
sudo killall memcached
bash run-servers.sh ${NODE_ID}
