package com.bromberger.graphxutils

import com.bromberger.graphxutils.GraphXHelper._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}

/**
  * Created by erik on 2/17/17.
  */
class DiGraphTest extends WordSpec with Matchers {
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("httpclient").setLevel(Level.OFF)
  Logger.getLogger("org").setLevel(Level.OFF)

  implicit lazy val spark = SparkSession.builder
    .master("local[*]")
    .config("spark.default.parallelism", 1)
    .getOrCreate()
  implicit lazy val sc = spark.sparkContext
  
  "the small graph digraph" should {
    "set up spark" in {
      sc.defaultParallelism shouldEqual 1
    }
    "create graphs with sane numbers of partitions" in {
      val g = SmallGraphs.pathDiGraph(30)
      g.edges.partitions.length shouldEqual 1
      g.vertices.partitions.length shouldEqual 1
      val paths = g.allPairsShortestPaths()
      paths.partitions.length shouldEqual 1
    }
    "shut down spark" in {
      Thread.sleep(1000000)
      spark.stop()
    }
  }
}
