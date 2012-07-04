import com.twitter.sbt._

name := "riak-tools"

version := "1.0"

scalaVersion := "2.9.1"

resolvers += "sonatype-public" at "https://oss.sonatype.org/content/groups/public"

libraryDependencies ++= Seq(
  "com.basho.riak" % "riak-client" % "1.0.5",
  "com.github.scopt" %% "scopt" % "2.1.0"
)


seq(GitProject.gitSettings: _*)

seq(PackageDist.newSettings: _*)
