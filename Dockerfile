FROM java:8

COPY target/scala-2.11/data-processor-assembly-1.0.jar /opt

WORKDIR /opt

ENTRYPOINT ["java", "-jar", "/opt/data-processor-assembly-1.0.jar"]