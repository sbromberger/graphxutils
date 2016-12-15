package com.bromberger.graphxutils

import org.apache.commons.rng.simple.RandomSource
import org.apache.spark.SparkContext
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

    /**
      * Returns the outgoing edges / arcs for a given vertex.
      * @param v  The vertex to query
      * @return   An RDD containing the outgoing edges
      */
    def outEdges(v: VertexId): RDD[Edge[ED]] = g.edges.filter(_.srcId == v)

    /**
      * Returns the incoming edges / arcs for a given vertex.
      * @param v   The vertex to query
      * @return    An RDD containing the incoming edges
      */
    def inEdges(v: VertexId): RDD[Edge[ED]] = g.edges.filter(_.dstId == v)

    /**
      * Returns the outgoing triplets for a given vertex.
      * @param v    The vertex to query
      * @return     An RDD containing the outgoing triplets
      */
    def outTriplets(v: VertexId): RDD[EdgeTriplet[VD, ED]] = g.triplets.filter(_.srcId == v)

    /**
      * Returns the incoming triplets for a given vertex.
      * @param v    The vertex to query
      * @return     An RDD containing the incoming triplets
      */
    def inTriplets(v: VertexId): RDD[EdgeTriplet[VD, ED]] = g.triplets.filter(_.dstId == v)

    /**
      * Returns the outgoing neighbors for a given vertex.
      * @param v    The vertex to query
      * @return     An RDD containing the outgoing neighbors
      */
    def outNeighbors(v: VertexId): RDD[(VertexId, VD)] = g.outTriplets(v).map(t => (t.dstId, t.dstAttr))

    /**
      * Returns the incoming neighbors for a given vertex.
      * @param v    The vertex to query
      * @return     An RDD containing the incoming neighbors
      */
    def inNeighbors(v: VertexId): RDD[(VertexId, VD)] = g.inTriplets(v).map(t => (t.srcId, t.srcAttr))

    /**
      * Creates a graph that is the union of the edge and vertex RDDs of this graph and another.
      * @param that   The graph with which the union should be performed
      * @return       A GraphX graph
      */
    def union(that:Graph[VD, ED]): Graph[VD, ED] = Graph(g.triplets.union(that.triplets))

    /**
      * Creates an BFS egoNet of a given depth starting at a specified vertex. Uses pregel.
      * @param s  The starting vertex for the egoNet
      * @param n  The depth of the egoNet (0 is the starting vertex itself)
      * @return   A GraphX representation of the egoNet
      */
    def egoNet(s: VertexId, n:Long): Graph[VD, ED] = {
      val initialMsg = Long.MinValue
      val newv = g.vertices.map(v => (v._1, (v._2, -1.toLong)))
      val pregelg = org.apache.spark.graphx.Graph[(VD, Long), ED](newv, g.edges)

      def vprog(v: VertexId, value: (VD, Long), message: Long): (VD, Long) = {
        if (v == s) (value._1, n)
        else (value._1, message)
      }

      def sendMsg(triplet: EdgeTriplet[(VD, Long), ED]): Iterator[(VertexId, Long)] = {
        val dstVertexId = triplet.dstId
        val srcVal = triplet.srcAttr._2
        val dstVal = triplet.dstAttr._2

        if ((srcVal > 0) && (dstVertexId != s) && (dstVal < 0))
          Iterator[(VertexId, Long)]((dstVertexId, srcVal - 1))
        else
          Iterator.empty
      }

      def mergeMsg(m1: Long, m2: Long) = m1.max(m2)

      val pregelRun = pregelg.pregel(initialMsg)(vprog, sendMsg, mergeMsg)
      val pregelSub = pregelRun.subgraph(vpred = (_, vattr) => vattr._2 > 0)
      org.apache.spark.graphx.Graph[VD, ED](pregelSub.vertices.map(v => (v._1, v._2._1)), pregelSub.edges)
    }

    /**
      * Calculates geodesic distances from a starting vertex. Uses pregel.
      * @param s    Starting vertex
      * @return     An RDD of (VertexId, Long) tuples representing the
      *             geodesic distance from the starting vertex to the VertexId.
      */
    def gDistances(s: VertexId): RDD[(VertexId, Long)] = {
      val initialMsg = -1L
      val newv = g.vertices.map(v => (v._1, (v._2, initialMsg)))
      val pregelg = org.apache.spark.graphx.Graph[(VD, Long), ED](newv, g.edges)

      def vprog(v:VertexId, value: (VD, Long), message: Long): (VD, Long) = {
        if (v == s) (value._1, 0L)
        else (value._1, message)
      }

      def sendMsg(triplet: EdgeTriplet[(VD, Long), ED]): Iterator[(VertexId, Long)] = {
        val dstVertexId = triplet.dstId
        val srcVal = triplet.srcAttr._2
        val dstVal = triplet.dstAttr._2

        if ((srcVal != initialMsg) && ((dstVal == initialMsg) || (dstVal > srcVal + 1)))
          Iterator[(VertexId, Long)]((dstVertexId, srcVal + 1))
        else Iterator.empty
      }

      def mergeMsg(m1: Long, m2: Long) = m1.min(m2)

      val pregelRun = pregelg.pregel(initialMsg)(vprog, sendMsg, mergeMsg)
      pregelRun.vertices.map(v => v._1 -> v._2._2)
    }
  }

  implicit class SmallGraphs(sc: SparkContext) {
    private def makeNodesFrom(r:Seq[Long]) : RDD[(VertexId, Unit)] = sc.parallelize(r.map(v => (v, ())))
    private def makeNodes(n:Long) : RDD[(VertexId, Unit)] = makeNodesFrom(0L.until(n))

    private def makeEdgesFrom(s:Seq[(Long, Long)]): RDD[Edge[Unit]] =
      sc.parallelize(s.map(e => Edge(e._1, e._2, ())))

    private val r = RandomSource.create(RandomSource.MT)

    @tailrec
    private def genNPairs(nPairs:Long, maxVal:Long, ordered:Boolean = false, pairs:Set[(Long, Long)] = Set[(Long, Long)]()) : Seq[(Long, Long)] = {
      def genPair(n: Long, ordered:Boolean = false): (Long, Long) = {
        val (x, y) = (r.nextLong(n), r.nextLong(n))
        if (x == y) genPair(n)
        else if (ordered && (y < x)) (y, x) else (x, y)
      }

      if (pairs.size == nPairs) pairs.toList
      else genNPairs(nPairs, maxVal, ordered, pairs + genPair(maxVal, ordered))
    }

    /**
      * A directed circle graph with a given number of nodes.
      * @param n    Number of nodes in the circle graph.
      * @return     A GraphX graph
      */
    def cycleDiGraph(n:Long): Graph[Unit, Unit] = {
      val r = 0L.until(n)
      val nodes = makeNodes(n)
      val edges : RDD[Edge[Unit]] = sc.parallelize(r.map(n => Edge(n, r.start + (n-r.start +1) % r.length, ())))
      Graph(nodes, edges)
    }

    /**
      * A directed path graph of a given length.
      * @param n    Length of the path graph
      * @return     A GraphX graph
      */
    def pathDiGraph(n:Long): Graph[Unit, Unit] = {
      val r = 0L.until(n)
      val rLen = r.length - 1
      val nodes = makeNodes(n)
      val edges: RDD[Edge[Unit]] = sc.parallelize(0.until(rLen).map(i => Edge(r(i), r(i+1), ())))
      Graph(nodes, edges)
    }

    /**
      * A directed wheel graph with a given number of nodes. VertexId 0 is the center node.
      * @param n  Number of nodes in the wheel graph, including the center node
      * @return   A GraphX graph
      */
    def wheelDiGraph(n:Long): Graph[Unit, Unit] = {
      val wheel = cycleDiGraph(n-1)
      val nodes = makeNodes(n)
      val spokes: RDD[Edge[Unit]] = wheel.vertices.map(v => Edge(n - 1, v._1))
      val edges: RDD[Edge[Unit]] = wheel.edges.union(spokes).map(e => Edge((e.srcId + 1) % n, (e.dstId + 1) % n))
      Graph(nodes, edges)
    }

    /**
      * A directed house graph
      * @return   A Graphx graph
      */
    def houseDiGraph: Graph[Unit, Unit] = {
      val e: List[(Long, Long)] = List((0, 1), (0, 2), (1, 3), (2, 3), (2, 4), (3, 4))
      val edges = makeEdgesFrom(e)
      val nodes = makeNodes(5)
      Graph(nodes, edges)
    }

    def starDiGraph(n:Long): Graph[Unit, Unit] = {
      val nodes = makeNodes(n)
      val e = 1L.until(n).map(v => (0L, v))
      val edges = makeEdgesFrom(e)
      Graph(nodes, edges)
    }

//    def binaryTreeDiGraph(depth:Int): Graph[Int, Int] = {
//      def edgesFromRootAtDepth(v:VertexId, d:Int) = {
//        return List(Edge(v, )
//      }
//      val nodes = Math.pow(2, depth).toInt - 1
//      val edges =
//
//    }

    /**
      * A directed graph of a given order and size, with randomly-generated edges.
      * Note: the graph will not contain self-loops.
      * @param nv   The number of vertices in the graph
      * @param ne   The number of random directed edges / arcs to include in the graph
      * @return     A GraphX graph
      */
    def randomDiGraph(nv:Long, ne:Long): Graph[Unit, Unit] = {
      assert(ne <= nv *(nv-1), "Number of edges requested (" + ne + ") exceeds maximum possible (" + nv * (nv-1) + ")")
      val nodes = makeNodes(nv)
      val pairs = genNPairs(ne, nv).map(p => Edge(p._1, p._2, ()))
      Graph(nodes, sc.parallelize(pairs))
    }

    /**
      * An undirected graph of a given order and size, with randomly-generated edges.
      * @param nv   The number of vertices in the graph
      * @param ne   The number of undirected random edges to include in the graph
      * @return     A GraphX graph
      */
    def randomGraph(nv:Long, ne:Long): Graph[Unit, Unit] = {
      assert(ne <= nv / 2 *(nv-1), "Number of edges requested (" + ne + ") exceeds maximum possible (" + nv * (nv-1) / 2 + ")")
      val nodes = makeNodes(nv)
      val pairs = genNPairs(ne, nv, ordered=true).flatMap(p => Seq(Edge(p._1, p._2, ()), Edge(p._2, p._1, ())))
      Graph(nodes, sc.parallelize(pairs))
    }

    def completeGraph(n:Long): Graph[Unit, Unit] = {
      val nodes = makeNodes(n)
      val e = 0L.until(n).flatMap(i => 0.until(n).filter(j=> j != i).map(j => (i, j))).map(p=> (p._1, p._2))
      val edges = makeEdgesFrom(e)
      Graph(nodes, edges)
    }

    /**
      * An undirected path graph of a given length.
      * @param n  Length of the path graph
      * @return   A GraphX graph
      */
    def pathGraph(n:Long): Graph[Unit, Unit] = pathDiGraph(n).toUndirected

    /**
      * An undirected circle graph with a given number of nodes.
      * @param n    Number of nodes in the graph
      * @return     A GraphX graph
      */
    def cycleGraph(n:Long): Graph[Unit, Unit] = cycleDiGraph(n).toUndirected

    /**
      * An undirected wheel graph with a given number of nodes. VertexId 0 is the center node.
      * @param n    Number of nodes in the graph, including the center node
      * @return     A GraphX graph
      */
    def wheelGraph(n:Long): Graph[Unit, Unit] = wheelDiGraph(n).toUndirected

    /**
      * An undirected house graph.
      * @return     A GraphX graph
      */
    def houseGraph: Graph[Unit, Unit] = houseDiGraph.toUndirected
  }
}
