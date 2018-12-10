#!/usr/bin/env bash


mvn clean & mvn package -q -DskipTests

java  -cp target/engine_java-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.alibabacloud.polar_race.engine.common.tests.SstTest