#!/bin/bash
spark2-submit --class bdad.etl.AirDataETL --master yarn --deploy-mode cluster --num-executors 20 --driver-memory 8G --executor-memory 2G target/scala-2.11/bdad-final-project_2.11-0.1.jar
