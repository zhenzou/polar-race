#!/usr/bin/env bash


rsync -vlcr `pwd`  root@aliyun:proj/java --delete --progress --exclude-from exclude.list

ssh -o StrictHostKeyChecking=no -tt -p 22 root@aliyun "cd proj/java/engine_java && bash run_tester.sh"