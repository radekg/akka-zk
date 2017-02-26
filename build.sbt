import BuildDefaults._

name := "akka-zk"

scalaVersion := BuildDefaults.buildScalaVersion
version := BuildDefaults.buildVersion
organization := BuildDefaults.buildOrganization

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.1.3",
  "org.slf4j" % "log4j-over-slf4j" % "1.7.22",
  "io.dropwizard.metrics" % "metrics-core" % "3.1.0",
  "com.typesafe.akka" %% "akka-actor" % BuildDefaults.akkaVersion,
  "com.typesafe.akka" %% "akka-slf4j" % BuildDefaults.akkaVersion,
  "com.typesafe" % "config" % "1.3.1" excludeAll( ExclusionRule(organization = "org.slf4j") ),
  "org.apache.zookeeper" % "zookeeper" % "3.4.9" excludeAll( ExclusionRule(organization = "org.slf4j") ),
  "org.reactivestreams" % "reactive-streams" % "1.0.0",
  // test
  "org.scalatest" %% "scalatest" % "3.0.1" % "test" excludeAll( ExclusionRule(organization = "org.slf4j") ),
  "com.typesafe.akka" %% "akka-stream" % BuildDefaults.akkaVersion % "test",
  "com.typesafe.akka" %% "akka-testkit" % BuildDefaults.akkaVersion % "test",
  "org.apache.curator" % "curator-test" % "2.11.1" % "test" excludeAll( ExclusionRule(organization = "org.slf4j") )
)

coverageEnabled in Test := true

crossScalaVersions := Seq("2.11.8", "2.12.1")

publishMavenStyle := true

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/AppMinistry/akka-zk</url>
    <licenses>
      <license>
        <name>Apache 2</name>
        <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
      </license>
    </licenses>
    <scm>
      <connection>scm:git:git://github.com/AppMinistry/akka-zk.git</connection>
      <developerConnection>scm:git:git://github.com/AppMinistry/akka-zk.git</developerConnection>
      <url>https://github.com/AppMinistry/akka-zk/tree/master</url>
    </scm>
    <developers>
      <developer>
        <id>radekg</id>
        <name>Radoslaw Gruchalski</name>
        <url>http://gruchalski.com</url>
      </developer>
    </developers>
  )