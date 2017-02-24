name := "akka-zk"

version := "0.1.0"

scalaVersion := "2.12.1"

organization := "uk.co.appministry"

val akkaVersion = "2.4.17"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.22",
  "io.dropwizard.metrics" % "metrics-core" % "3.1.0",
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
  "com.typesafe" % "config" % "1.3.1" excludeAll( ExclusionRule(organization = "org.slf4j") ),
  "org.apache.zookeeper" % "zookeeper" % "3.4.9" excludeAll( ExclusionRule(organization = "org.slf4j") ),
  // test
  "org.scalatest" %% "scalatest" % "3.0.1" % "test" excludeAll( ExclusionRule(organization = "org.slf4j") ),
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test",
  "org.apache.curator" % "curator-test" % "2.11.1" % "test" excludeAll( ExclusionRule(organization = "org.slf4j") )
)