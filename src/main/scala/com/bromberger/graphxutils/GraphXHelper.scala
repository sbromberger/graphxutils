package com.bromberger.graphxutils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._
import org.apache.spark.mllib.random.RandomRDDs.uniformRDD
import org.apache.spark.rdd.RDD

import scala.annotation.tailrec
import scala.collection.immutable.Map
import scala.reflect.ClassTag

/*
 * Created by sbromberger on 2016-11-19.
 */


object GraphXHelper {

  private def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000.0 + "s")
    result
  }

  implicit class EdgeAdditions[ED:ClassTag](e: Edge[ED]) {
    def reverse(xform: ED => ED): Edge[ED] = Edge(e.dstId , e.srcId, xform(e.attr))
    def reverse: Edge[ED] = reverse(identity[ED])
  }

  implicit class GraphXAdditions[VD:ClassTag, ED:ClassTag](g: Graph[VD, ED]) extends Serializable {
    def Graph(tripletRDD: RDD[EdgeTriplet[VD, ED]]): org.apache.spark.graphx.Graph[VD, ED] = {
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
      *
      * @param v The vertex to query
      * @return An RDD containing the outgoing edges
      */
    def outEdges(v: VertexId): RDD[Edge[ED]] = g.edges.filter(_.srcId == v)

    /**
      * Returns the incoming edges / arcs for a given vertex.
      *
      * @param v The vertex to query
      * @return An RDD containing the incoming edges
      */
    def inEdges(v: VertexId): RDD[Edge[ED]] = g.edges.filter(_.dstId == v)

    /**
      * Returns the outgoing triplets for a given vertex.
      *
      * @param v The vertex to query
      * @return An RDD containing the outgoing triplets
      */
    def outTriplets(v: VertexId): RDD[EdgeTriplet[VD, ED]] = g.triplets.filter(_.srcId == v)

    /**
      * Returns the incoming triplets for a given vertex.
      *
      * @param v The vertex to query
      * @return An RDD containing the incoming triplets
      */
    def inTriplets(v: VertexId): RDD[EdgeTriplet[VD, ED]] = g.triplets.filter(_.dstId == v)

    /**
      * Returns the outgoing neighbors for a given vertex.
      *
      * @param v The vertex to query
      * @return An RDD containing the outgoing neighbors
      */
    def outNeighbors(v: VertexId): RDD[(VertexId, VD)] = g.outTriplets(v).map(t => (t.dstId, t.dstAttr))

    /**
      * Returns the incoming neighbors for a given vertex.
      *
      * @param v The vertex to query
      * @return An RDD containing the incoming neighbors
      */
    def inNeighbors(v: VertexId): RDD[(VertexId, VD)] = g.inTriplets(v).map(t => (t.srcId, t.srcAttr))

    /**
      * Creates a graph that is the union of the edge and vertex RDDs of this graph and another.
      *
      * @param that The graph with which the union should be performed
      * @return A GraphX graph
      */
    def union(that: Graph[VD, ED]): Graph[VD, ED] = Graph(g.triplets.union(that.triplets))

    /**
      * Creates an BFS egoNet of a given depth starting at a specified vertex. Uses pregel.
      *
      * @param s The starting vertex for the egoNet
      * @param n The depth of the egoNet (0 is the starting vertex itself)
      * @return A GraphX representation of the egoNet
      */
    def egoNet(s: VertexId, n: Long): Graph[VD, ED] = {
      val initialMsg = Long.MinValue
      val pregelg = g.mapVertices((_, vd) => (vd, -1L))

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
      *
      * @param s Starting vertex
      * @return An RDD of (VertexId, Long) tuples representing the
      *         geodesic distance from the starting vertex to the VertexId.
      */
    def gDistances(s: VertexId): RDD[(VertexId, Long)] = {
      val initialMsg = -1L
      val pregelg = g.mapVertices((_, vd) => (vd, initialMsg))

      def vprog(v: VertexId, value: (VD, Long), message: Long): (VD, Long) = {
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

    case class ParentDist(parent:VertexId, dist:Long) {
      def next = ParentDist(parent, dist + 1)
      def <(that:ParentDist): Boolean = dist < that.dist
      def min(that:ParentDist): ParentDist = if (dist < that.dist) this else that
      override def toString:String = "distance " + dist + ", parent " + parent
    }

    def allPairsShortestPaths(distFn: Edge[ED] => Double = e => 1): RDD[(VertexId, Map[VertexId, ParentDist])] = {
      val initialMsg = Map(-1L -> ParentDist(-1L, -1L))
      val pregelg = g.mapVertices((vid, vd) => (vd, Map[VertexId, ParentDist](vid -> ParentDist(vid, 0L)))).reverse
      def vprog(v: VertexId, value: (VD, Map[VertexId, ParentDist]), message: Map[VertexId, ParentDist]): (VD, Map[VertexId, ParentDist]) = {
        if (v == 0) println("--- NEW ITERATION ---")
        val updatedValues = mergeMsg(value._2, message).filter(v => v._2.dist >= 0)
        (value._1, updatedValues)
      }

      def sendMsg(triplet: EdgeTriplet[(VD, Map[VertexId, ParentDist]), ED]): Iterator[(VertexId, Map[VertexId, ParentDist])] = {
        val dstVertexId = triplet.dstId
        val srcMap = triplet.srcAttr._2
        val dstMap = triplet.dstAttr._2  // guaranteed to have dstVertexId as a key

        val updatesToSend : Map[VertexId, ParentDist] = srcMap.filter {
          case (vid, srcPD) => dstMap.get(vid) match {
            case Some(dstPD) => dstPD.dist > srcPD.dist + 1  && dstPD.parent != triplet.srcId  // if it exists, is it a new, cheaper path?
            case _ => true // not found - new update
          }
        }.map(u => u._1 -> ParentDist(triplet.srcId, u._2.dist +1))

        if (updatesToSend.nonEmpty) {
          println("sending " + updatesToSend.size + " messages from " + triplet.srcId + " to " + dstVertexId)
          updatesToSend.keys.foreach(k => println("  " + k + " -> " + updatesToSend(k)))
          Iterator[(VertexId, Map[VertexId, ParentDist])]((dstVertexId, updatesToSend))
        }
        else
          Iterator.empty
      }

      def mergeMsg(m1: Map[VertexId, ParentDist], m2: Map[VertexId, ParentDist]): Map[VertexId, ParentDist] = {

        def mergeOption[A](o1: Option[A], o2: Option[A])(f: (A, A) => A): A = if (o1.isDefined && o2.isDefined) f(o1.get, o2.get) else o1.orElse(o2).get
        def mergeMap[A, B](m1: Map[A, B], m2: Map[A, B])(f: (B, B) => B) = (m1.keySet ++ m2.keySet).iterator.map(k => (k, mergeOption(m1.get(k), m2.get(k))(f))).toMap

        mergeMap(m1, m2)(_ min _)
      }

      val pregelRun = pregelg.pregel(initialMsg)(vprog, sendMsg, mergeMsg)
      val sps = pregelRun.vertices.map(v => v._1 -> v._2._2)
      sps
    }
  }

  implicit class SmallGraphs(sc: SparkContext) {
    private val densityCutoff = 0.62
    private def makeNodesFrom(r:Seq[Long]) : RDD[(VertexId, Unit)] = sc.parallelize(r.map(v => (v, ())))
    private def makeNodes(n:Long) : RDD[(VertexId, Unit)] = makeNodesFrom(0L.until(n))

    private def makeEdgesFrom(s:Seq[(Long, Long)]): RDD[Edge[Unit]] =
      sc.parallelize(s.map(e => Edge(e._1, e._2, ())))
    /**
      * A directed cycle graph with a given number of nodes.
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
      val e: List[(Long, Long)] = List((0, 1), (0, 2), (1, 3), (2, 4), (3, 4))
      val edges = makeEdgesFrom(e)
      val nodes = makeNodes(5)
      Graph(nodes, edges)
    }

    /**
      * A directed star graph with a given number of nodes. VertexId 0 is the center node.
      * @param n    The number of nodes, including the center vertex
      * @return     A GraphX graph
      */
    def starDiGraph(n:Long): Graph[Unit, Unit] = {
      val nodes = makeNodes(n)
      val e = 1L.until(n).map(v => (0L, v))
      val edges = makeEdgesFrom(e)
      Graph(nodes, edges)
    }

    /**
      * A directed full binary tree of a given depth. VertexId 0 is the root node.
      * @param depth    The depth of the binary tree
      * @return         A GraphX graph
      */
    def binaryTreeDiGraph(depth:Long): Graph[Unit, Unit] = {
      val nNodes = Math.pow(2L, depth).toLong - 1L
      val nodes = makeNodes(nNodes)
      val e = depth.until(1).by(-1).flatMap(d => {
        val offsetNodeId = Math.pow(2, d-1).toLong - 1
        val nEdgesAtDepth = offsetNodeId + 1
        offsetNodeId.until(offsetNodeId + nEdgesAtDepth).map(v => ((v-1) / 2, v))
      })
      val edges = makeEdgesFrom(e)
      Graph(nodes, edges)
    }

    /**
      * Makes an RDD of Pairs of Long that are distinct and unique.
      * @param n    The number of pairs to make
      * @param nv   The maximum value for each element in the pair
      * @param ordered  True if the pairs should be ordered
      */
    private def makePairs(n:Long, nv: Long, ordered: Boolean = false): RDD[(Long, Long)] = {
      if (n == 0) sc.parallelize(List[(Long, Long)]())
      else {
        val nv2 = nv * nv
        var i = 0

        @tailrec
        def makeTheRest(remaining: Long, currRDD: RDD[(Long, Long)]): RDD[(Long, Long)] = {
          assert(remaining >= 0, "Whoops - we have overshot by " + (-remaining) + " elements!")
          println("in mtr with " + remaining + " remaining")
          if (remaining == 0) currRDD
          else {
            i += 1
            val newRDD = uniformRDD(sc, remaining, ((n / Int.MaxValue) + 1).toInt)
            val newPairRDD = newRDD.map(v => (nv2 * v).toLong).map(v => (v / nv, v % nv)).filter(p => p._1 != p._2)
              .map(p => if (ordered && (p._1 > p._2)) p.swap else p).distinct
            val unionedRDD = currRDD.union(newPairRDD).distinct
            makeTheRest(n - unionedRDD.count, unionedRDD)
          }
        }

        val initialRDD = uniformRDD(sc, n, ((n / Int.MaxValue) + 1).toInt)
          .map(v => (nv2 * v).toLong).map(v => (v / nv, v % nv)).filter(p => p._1 != p._2)
          .map(p => if (ordered && (p._1 > p._2)) p.swap else p).distinct

        makeTheRest(n - initialRDD.count, initialRDD)
      }
    }

    /**
      * A directed graph of a given order and size, with randomly-generated edges.
      * Note: the graph will not contain self-loops.
      * @param nv   The number of vertices in the graph
      * @param ne   The number of random directed edges / arcs to include in the graph
      * @return     A GraphX graph
      */
    def randomDiGraph(nv:Long, ne:Long): Graph[Unit, Unit] = {
      val maxPossibleEdges = nv * (nv-1)
      assert(ne <= maxPossibleEdges, "Number of edges requested (" + ne + ") exceeds maximum possible (" + maxPossibleEdges + ")")
      val nodes = makeNodes(nv)
      val edgeRDD = if (ne < maxPossibleEdges * densityCutoff)
        makePairs(ne, nv).map(p => Edge(p._1, p._2, ()))
      else {    // dense graph
        val allPairs = allPairsRDD(nv)
        println("making " + (maxPossibleEdges - ne) + " pairs")
        allPairs.subtract(makePairs(maxPossibleEdges - ne, nv)).map(p => Edge(p._1, p._2, ()))
        }
      Graph(nodes, edgeRDD)
    }

    /**
      * An undirected graph of a given order and size, with randomly-generated edges.
      * @param nv   The number of vertices in the graph
      * @param ne   The number of undirected random edges to include in the graph
      * @return     A GraphX graph
      */
    def randomGraph(nv:Long, ne:Long): Graph[Unit, Unit] = {
      val maxPossibleEdges = nv * (nv-1) / 2
      assert(ne <= maxPossibleEdges, "Number of edges requested (" + ne + ") exceeds maximum possible (" + maxPossibleEdges + ")")
      val nodes = makeNodes(nv)
      println("ne = " + ne, "mPE * " + densityCutoff + " = " + (maxPossibleEdges * densityCutoff))
      val edgeRDD = if (ne < maxPossibleEdges * densityCutoff)
          makePairs(ne, nv, ordered = true).map(p => Edge(p._1, p._2, ()))
      else {        // dense graph
        val allPairs = allPairsRDD(nv).filter(p => p._1 > p._2)
        println("allPairs for undirected = " + allPairs.count() + ", making " + (maxPossibleEdges - ne) + " pairs")
        val undirectedPairsToRemove = makePairs(maxPossibleEdges - ne, nv, ordered = true)
        val pairsToRemove = undirectedPairsToRemove.union(undirectedPairsToRemove.map(p => p.swap))
        allPairs.subtract(pairsToRemove).map(p => Edge(p._1, p._2, ()))
      }
      Graph(nodes, edgeRDD.union(edgeRDD.map(e => e.reverse)))
    }

    /**
      * Creates an RDD of pairs representing all (non-self-looped) edges for n vertices
      * @param n  Number of vertices
      * @return   an RDD of Pairs
      */
    private def allPairsRDD(n:Long): RDD[(Long, Long)] = {
      val nRDD = sc.parallelize(0L.until(n))
      nRDD.cartesian(nRDD).filter(p => p._1 != p._2)
    }

    /**
      * An undirected complete graph (all pairs of nodes interconnected).
      * @param n  The number of vertices in the graph
      * @return   A GraphX graph
      */
    def completeGraph(n:Long): Graph[Unit, Unit] = {
      val nodes = makeNodes(n)
      val edges = allPairsRDD(n).map(p => Edge(p._1, p._2, ()))

      Graph(nodes, edges)
    }

    /**
      * An undirected path graph of a given length.
      * @param n  Length of the path graph
      * @return   A GraphX graph
      */
    def pathGraph(n:Long): Graph[Unit, Unit] = pathDiGraph(n).toUndirected

    /**
      * An undirected cycle graph with a given number of nodes.
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

    /**
      * An undirected star graph with a given number of nodes. VertexId 0 is the center node.
      * @param n    The number of nodes, including the center vertex
      * @return     A GraphX graph
      */
    def starGraph(n:Long): Graph[Unit, Unit] = starDiGraph(n).toUndirected

    /**
      * An undirected full binary tree of a given depth. VertexId 0 is the root node.
      * @param depth    The depth of the binary tree
      * @return         A GraphX graph
      */
    def binaryTreeGraph(depth:Long): Graph[Unit, Unit] = binaryTreeDiGraph(depth).toUndirected
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("com").setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("bromberger").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val r = new scala.util.Random

    def runOneDiGraphTest(n:Int): Unit = {
      val nv = 2.max(r.nextInt(n))
      val ne = 1.max(r.nextInt(nv) * r.nextInt(nv))
      println("running digraph with (" + nv + ", " + ne + ")")
      val g = sc.randomDiGraph(nv, ne)
      val vct = g.vertices.count()
      val ect = g.edges.count()
      assert(vct == nv, "vct " + vct + " != nv " + nv)
      assert(ect == ne, "ect " + ect + " != ne " + ne)
    }

    def runOneGraphTest(n:Int): Unit = {
      val nv = 2.max(r.nextInt(n))
      val ne = 1.max(r.nextInt(nv) * r.nextInt(nv) / 2)
      println("running graph with (" + nv + ", " + ne + ")")
      val g = sc.randomGraph(nv, ne)
      val vct = g.vertices.count()
      val ect = g.edges.count()
      assert(vct == nv, "vct " + vct + " != nv " + nv)
      assert(ect == 2 * ne, "ect " + ect + " != 2ne " + (ne * 2))
    }


    1.to(80).foreach(i => {
      runOneDiGraphTest(10 * i)
      runOneGraphTest(10 * i)
      println("Test " + i + " ok")
    })
  }
}
