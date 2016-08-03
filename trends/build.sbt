name := """trend-service"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test, 
  "com.datastax.cassandra" % "cassandra-driver-core" % "3.0.0"
)

resolvers += "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"


assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

