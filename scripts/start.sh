#!/usr/bin/env bash

wait-for-it.sh db:9042 -- java -jar /opt/data-processor-assembly-1.0.jar
