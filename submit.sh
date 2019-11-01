#!/bin/bash
# spark2-submit --name "AirDataETL" --class bdad.etl.AirDataETL --master yarn --num-executors 30 --driver-memory 5G --executor-memory 2G target/scala-2.11/bdad-final-project_2.11-0.1.jar

spark2-submit --name "AirDataTest" --class bdad.etl.airdata.AirDatasetTest --master yarn --deploy-mode client --verbose --driver-memory 5G --executor-memory 2G --num-executors 10 target/scala-2.11/bdad-final-project_2.11-0.1.jar
