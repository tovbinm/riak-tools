sbtVersion := "0.11.2"

addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "2.1.0-RC1")

addSbtPlugin("com.twitter" % "sbt-package-dist" % "1.0.6")

resolvers += "Twitter Repo" at "http://maven.twttr.com/"