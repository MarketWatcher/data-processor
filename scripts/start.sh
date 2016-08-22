#!/usr/bin/env bash

wait-for-it.sh db:9042 -- "spark-submit --master local --class TwitterProcessor /opt/data-processor.jar"
