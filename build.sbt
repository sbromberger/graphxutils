name := "graphxutils"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.2"
//libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.0.2"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.2"


resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"
