organization := "com.libitec"

name := "util.amqp"

version := "0.0.4-SNAPSHOT"

scalaVersion := "2.9.2"

libraryDependencies ++= Seq(
    "javax.inject" % "javax.inject" % "1",
    "com.typesafe" % "config" % "0.5.0",
    "com.rabbitmq" % "amqp-client" % "2.7.1",
    "org.scalatest" %% "scalatest" % "1.8" % "test"
)
