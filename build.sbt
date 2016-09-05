name := "data-processor"

version := "1.0"

scalaVersion := "2.10.5"

fork := true

coverageEnabled in Test := true

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.10" % "1.6.2" copy (isChanging = false),
  "org.apache.spark" %% "spark-streaming" % "1.6.2" copy (isChanging = false),
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.2" copy (isChanging = false),
  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.2" copy (isChanging = false),
  "org.apache.spark" %% "spark-sql" % "1.6.2" copy (isChanging = false),
  "com.datastax.spark" %% "spark-cassandra-connector" % "1.6.0" copy (isChanging = false),
  "org.scalatest" %% "scalatest" % "3.0.0"  % "test",
  "com.datastax.spark" %% "spark-cassandra-connector-embedded" % "1.1.2" copy (isChanging = false),
  "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test" copy (isChanging = false)
)

dependencyOverrides ++= Set(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
)

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.**" -> "shadeio.@1").inAll
)

assemblyJarName in assembly := s"${name.value}-${version.value}.jar"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
