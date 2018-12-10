#!/usr/bin/env bash

rsync -vlcr `pwd`  root@aliyun:proj/java  --progress --delete --exclude-from exclude.list

ssh -o StrictHostKeyChecking=no -tt -p 22 root@aliyun "cd proj/java/engine_java && bash run_sst.sh"