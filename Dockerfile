FROM sequenceiq/spark:1.6.0

COPY target/scala-2.10/data-processor-assembly-1.0.jar /opt/data-processor.jar

CMD spark-submit --master local --class TwitterProcessor /opt/data-processor.jar

