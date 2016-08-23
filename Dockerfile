FROM gettyimages/spark:1.6.2-hadoop-2.6

COPY target/scala-2.10/data-processor-1.0.jar /opt/data-processor.jar

ENTRYPOINT spark-submit --master local --class TwitterProcessor /opt/data-processor.jar

