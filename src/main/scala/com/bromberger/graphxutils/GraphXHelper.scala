package com.bromberger.graphxutils

import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD

/**
  * Created by sbromberger on 2016-11-19.
  */
object GraphXHelper {
  implicit class GraphXAdditions[VD, ED](g: org.apache.spark.graphx.Graph[VD, ED]) {
    def outEdges(s: VertexId): RDD[Edge[ED]] = g.edges.filter(e => e.srcId == s)
    def inEdges(s: VertexId): RDD[Edge[ED]] = g.edges.filter(e => e.dstId == s)

    def outNeighbors(s: VertexId): RDD[(VertexId, VD)] = g.triplets.filter(t => t.srcId == s).map(t => (t.dstId, t.dstAttr))
    def inNeighbors(s: VertexId): RDD[(VertexId, VD)] = g.triplets.filter(t => t.dstId == s).map(t => (t.srcId, t.srcAttr))
  }
}
