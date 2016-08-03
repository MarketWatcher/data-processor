FROM java:8

COPY target/scala-2.11/data-processor-assembly-1.0.jar /opt/processor.jar

WORKDIR /opt

ENTRYPOINT ["java", "-jar", "/opt/processor.jar"]
