import com.twitter.sbt._

name := "riak-tools"

version := "1.0"

scalaVersion := "2.9.1"

resolvers ++= Seq(
	"sonatype-public" at "https://oss.sonatype.org/content/groups/public",
	"Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= Seq(
  "com.basho.riak" % "riak-client" % "1.0.5",
  "com.github.scopt" %% "scopt" % "2.1.0",
  "com.typesafe.akka" % "akka-actor" % "2.0.2"
)


seq(GitProject.gitSettings: _*)

seq(PackageDist.newSettings: _*)
