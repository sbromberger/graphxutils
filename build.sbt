name := "graphxutils"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.2"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.0.2"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.0.2"
libraryDependencies += "org.apache.commons" % "commons-rng-parent" % "1.0"
libraryDependencies += "org.apache.commons" % "commons-rng-core" % "1.0"
libraryDependencies += "org.apache.commons" % "commons-rng-simple" % "1.0"

resolvers += "mvnrepository" at "http://mvnrepository.com/artifact/"
