name := "graphxutils"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"

testOptions in Test += Tests.Argument(TestFrameworks.ScalaTest, "-oDS")
