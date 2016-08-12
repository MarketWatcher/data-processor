name := "data-processor"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.1" % "provided" copy (isChanging = false),
  "org.apache.spark" %% "spark-streaming" % "1.6.1" % "provided" copy (isChanging = false),
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.1" % "provided" copy (isChanging = false),
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.1" copy (isChanging = false),
  "org.apache.spark" %% "spark-sql" % "1.6.1" % "provided" copy (isChanging = false),
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0" copy (isChanging = false),
  "spark.jobserver" %% "job-server-api" % "0.6.2" % "provided" copy (isChanging = false)
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}