#!/usr/bin/env bash
JVM_OPS="-server -Xms2560m -Xmx2560m -XX:MaxDirectMemorySize=512m -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:-UseBiasedLocking -XX:+PrintGCTimeStamps  -XX:+PrintGCDetails"


echo "JVM_OPS ${JVM_OPS}"

export tester_engine_path=/tmp/kv
#export tester_engine_path=E:\\Projects\\Java\\jpolar\\logs
export tester_engine_thread=64
export tester_value_size=4096
export tester_value_num=10001


mkdir -p ${tester_engine_path}
#rm -rf ${tester_engine_path}/*
rm -rf ${tester_engine_path}
mvn clean & mvn package -q -DskipTests

#java ${JVM_OPS} -cp target/engine_java-0.0.1-SNAPSHOT.jar com.alibabacloud.polar_race.engine.common.tests.Tester
#java  $@ ${JVM_OPS} -jar target/engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar

java  $@ ${JVM_OPS} -cp target/engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.engine.common.tests.RandomTester
echo "java  $@  ${JVM_OPS} -jar target/engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar"




