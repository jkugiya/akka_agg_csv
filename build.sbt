name := "Akka CSV Aggregate"

version := "1.0"

scalaVersion := "2.12.4"

resolvers += "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.5.9",
  "com.lightbend.akka" %% "akka-stream-alpakka-csv" % "0.13",
  "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.13",
  "org.apache.commons" % "commons-lang3" % "3.7"
)