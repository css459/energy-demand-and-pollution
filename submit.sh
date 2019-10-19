#!/bin/bash
spark2-submit --name "AirDataETL" --class bdad.etl.AirDataETL --master yarn --num-executors 30 --driver-memory 5G --executor-memory 2G target/scala-2.11/bdad-final-project_2.11-0.1.jar
