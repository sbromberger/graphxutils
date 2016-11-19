package com.bromberger.graphxutils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.reflect.ClassTag

/*
 * Created by sbromberger on 2016-11-19.
 */


object GraphXHelper {
  implicit class EdgeAdditions[ED:ClassTag](e: Edge[ED]) {
    def reverse(xform: ED => ED): Edge[ED] = Edge(e.dstId , e.srcId, xform(e.attr))
    def reverse: Edge[ED] = reverse(identity[ED])
  }

  implicit class GraphXAdditions[VD:ClassTag, ED:ClassTag](g: Graph[VD, ED]) extends Serializable {
    def Graph(tripletRDD:RDD[EdgeTriplet[VD, ED]]): org.apache.spark.graphx.Graph[VD, ED] = {
      val nodes = tripletRDD.flatMap(t => List((t.srcId, t.srcAttr), (t.dstId, t.dstAttr))).distinct
      val edges = tripletRDD.map(t => Edge[ED](t.srcId, t.dstId, t.attr))
      org.apache.spark.graphx.Graph(nodes, edges)
    }

    def toUndirected: Graph[VD, ED] = {
      val reve = g.edges.map(e => e.reverse)
      org.apache.spark.graphx.Graph(g.vertices, g.edges.union(reve))
    }

    val emptyVertexRDD: RDD[VD] = g.vertices.sparkContext.emptyRDD[VD]
    val emptyEdgeRDD: RDD[Edge[ED]] = g.edges.sparkContext.emptyRDD[Edge[ED]]
    val emptyTripletRDD: RDD[EdgeTriplet[VD, ED]] = g.triplets.sparkContext.emptyRDD[EdgeTriplet[VD, ED]]

    def outEdges(v: VertexId): RDD[Edge[ED]] = g.edges.filter(_.srcId == v)
    def inEdges(v: VertexId): RDD[Edge[ED]] = g.edges.filter(_.dstId == v)

    def outTriplets(v: VertexId): RDD[EdgeTriplet[VD, ED]] = g.triplets.filter(_.srcId == v)
    def inTriplets(v: VertexId): RDD[EdgeTriplet[VD, ED]] = g.triplets.filter(_.dstId == v)

    def outNeighbors(v: VertexId): RDD[(VertexId, VD)] = g.outTriplets(v).map(t => (t.dstId, t.dstAttr))
    def inNeighbors(v: VertexId): RDD[(VertexId, VD)] = g.inTriplets(v).map(t => (t.srcId, t.srcAttr))

    def union(that:Graph[VD, ED]): Graph[VD, ED] = Graph(g.triplets.union(that.triplets))

    def egoNet(s: VertexId, n:Int): Graph[VD, ED] = {
      val initialMsg = Int.MinValue
      val newv = g.vertices.map(v => (v._1, (v._2, -1)))
      val pregelg = org.apache.spark.graphx.Graph[(VD, Int), ED](newv, g.edges)

      def vprog(v: VertexId, value: (VD, Int), message: Int): (VD, Int) = {
        if (v == s) (value._1, n)
        else (value._1, message)
      }

      def sendMsg(triplet: EdgeTriplet[(VD, Int), ED]): Iterator[(VertexId, Int)] = {
        val dstVertexId = triplet.dstId
        val srcVal = triplet.srcAttr._2
        val dstVal = triplet.dstAttr._2

        if ((srcVal > 0) && (dstVertexId != s) && (dstVal < 0))
          Iterator[(VertexId, Int)]((dstVertexId, srcVal - 1))
        else
          Iterator.empty
      }

      def mergeMsg(m1: Int, m2: Int) = m1.max(m2)

      val pregelRun = pregelg.pregel(initialMsg)(vprog, sendMsg, mergeMsg)
      val pregelSub = pregelRun.subgraph(vpred = (_, vattr) => vattr._2 > 0)
      org.apache.spark.graphx.Graph[VD, ED](pregelSub.vertices.map(v => (v._1, v._2._1)), pregelSub.edges)
    }
  }

  implicit class SmallGraphs(sc: SparkContext) {
    private def makeNodesFrom(r:Seq[Int]) : RDD[(VertexId, Int)] = sc.parallelize(r.map(v => (v.toLong, v)))
    private def makeNodes(n:Int) : RDD[(VertexId, Int)] = makeNodesFrom(0.until(n))

    private def makeEdgesFrom(s:Seq[(Int, Int)], v:Int = 1): RDD[Edge[Int]] =
      sc.parallelize(s.map(e => Edge(e._1, e._2, v)))
    private val r = new scala.util.Random

    @tailrec
    private def genNPairs(nPairs:Int, maxVal:Int, ordered:Boolean = false, pairs:Set[(Int, Int)] = Set[(Int, Int)]()) : Seq[(Int, Int)] = {
      def genPair(n: Int, ordered:Boolean = false): (Int, Int) = {
        val (x, y) = (r.nextInt(n), r.nextInt(n))
        if (x == y) genPair(n)
        else if (ordered && (y < x)) (y, x) else (x, y)
      }

      if (pairs.size == nPairs) pairs.toList
      else genNPairs(nPairs, maxVal, ordered, pairs + genPair(maxVal, ordered))
    }

    def circleDiGraph(n:Int): Graph[Int, Int] = {
      val r = 0.until(n)
      val nodes = makeNodes(n)
      val edges : RDD[Edge[Int]] = sc.parallelize(r.map(n => Edge(n, r.start + (n-r.start +1) % r.length, 1)))
      Graph(nodes, edges)
    }

    def pathDiGraph(n:Int): Graph[Int, Int] = {
      val r = 0.until(n)
      val rLen = r.length
      val nodes = makeNodes(n)
      val edges: RDD[Edge[Int]] = sc.parallelize(0.until(rLen).map(i => Edge(r(i), r(i+1), 1)))
      Graph(nodes, edges)
    }


    def wheelDiGraph(n:Int): Graph[Int, Int] = {
      val wheel = circleDiGraph(n-1)
      val nodes = makeNodes(n)
      val spokes: RDD[Edge[Int]] = wheel.vertices.map(v => Edge(n - 1, v._1))
      val edges: RDD[Edge[Int]] = wheel.edges.union(spokes).map(e => Edge((e.srcId + 1) % n, (e.dstId + 1) % n, 1))
      Graph(nodes, edges)
    }

    def houseDiGraph: Graph[Int, Int] = {
      val e = List((0, 1), (0, 2), (1, 3), (2, 3), (2, 4), (3, 4))
      val edges = makeEdgesFrom(e)
      val nodes = makeNodes(5)
      Graph(nodes, edges)
    }

    def randomDiGraph(nv:Int, ne:Int, edgeVal:Int = 1): Graph[Int, Int] = {
      assert(ne.toLong <= (nv.toLong *(nv-1)), "Number of edges requested (" + ne + ") exceeds maximum possible (" + nv * (nv-1) + ")")
      val nodes = makeNodes(nv)
      val pairs = genNPairs(ne, nv).map(p => Edge(p._1, p._2, edgeVal))
      Graph(nodes, sc.parallelize(pairs))
    }

    def randomGraph(nv:Int, ne:Int, edgeVal:Int = 1): Graph[Int, Int] = {
      assert(ne.toLong <= nv.toLong / 2 *(nv-1), "Number of edges requested (" + ne + ") exceeds maximum possible (" + nv * (nv-1) / 2 + ")")
      val nodes = makeNodes(nv)
      val pairs = genNPairs(ne, nv, ordered=true).flatMap(p => Seq(Edge(p._1, p._2, edgeVal), Edge(p._2, p._1, edgeVal)))
      println("pairs length = " + pairs.length)
      Graph(nodes, sc.parallelize(pairs))
    }


    def pathGraph(n:Int): Graph[Int, Int] = pathDiGraph(n).toUndirected
    def circleGraph(n:Int): Graph[Int, Int] = circleDiGraph(n).toUndirected
    def wheelGraph(n:Int): Graph[Int, Int] = wheelDiGraph(n).toUndirected
    def houseGraph: Graph[Int, Int] = houseDiGraph.toUndirected
  }
}
