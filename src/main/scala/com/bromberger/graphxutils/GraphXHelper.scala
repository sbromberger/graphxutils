package com.bromberger.graphxutils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Edge, EdgeTriplet, VertexId, Graph, PartitionStrategy}
import org.apache.spark.mllib.random.RandomRDDs.uniformRDD
import org.apache.spark.rdd.RDD

import scala.collection.immutable.Map
import scala.reflect.ClassTag

/*
 * Created by sbromberger on 2016-11-19.
 */

object SG {
  private def makeNodesFrom(r:Seq[Long])(implicit sc:SparkContext): RDD[(VertexId, Unit)] = sc.parallelize(r.map(v => (v, ())))
  private def makeNodes(n:Long)(implicit sc:SparkContext): RDD[(VertexId, Unit)] = makeNodesFrom(0L.until(n))

  private def makeEdgesFrom(s:Seq[(Long, Long)])(implicit sc:SparkContext): RDD[Edge[Unit]] =
    sc.parallelize(s.map(e => Edge(e._1, e._2, ())))
  /**
    * A directed cycle graph with a given number of nodes.
    * @param n    Number of nodes in the circle graph.
    * @return     A GraphX graph
    */
  def cycleDG(n:Long)(implicit sc:SparkContext): Graph[Unit, Unit] = {
    val r = 0L.until(n)
    val nodes = makeNodes(n)
    val edges : RDD[Edge[Unit]] = sc.parallelize(r.map(n => Edge(n, r.start + (n-r.start +1) % r.length, ())))
    Graph(nodes, edges)
  }

}

object GraphXHelper {

  def time[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000000.0 + "s")
    result
  }

  implicit class RDDAdditions[T: ClassTag](rdd: RDD[T]) extends Serializable {
    /**
      * Returns an RDD that represents a fixed-length subset of the original.
      * @param n  Number of elements in the RDD to limit
      * @return   an RDD of size `n`
      */
    def truncate(n: Long): RDD[T] = rdd.zipWithIndex.collect{case (v: T, i: Long) if i < n => v}
  }
  implicit class GraphXAdditions[VD: ClassTag, ED: ClassTag](g: Graph[VD, ED]) extends Serializable {
    def Graph(tripletRDD: RDD[EdgeTriplet[VD, ED]]): org.apache.spark.graphx.Graph[VD, ED] = {
      val nodes = tripletRDD.flatMap(t => List((t.srcId, t.srcAttr), (t.dstId, t.dstAttr))).distinct
      val edges = tripletRDD.map(t => Edge[ED](t.srcId, t.dstId, t.attr))
      org.apache.spark.graphx.Graph(nodes, edges)
    }

    def toUndirected: Graph[VD, ED] =
      org.apache.spark.graphx.Graph[VD, ED](g.vertices, g.edges.union(g.edges.reverse))

    val emptyVertexRDD: RDD[VertexId] = g.vertices.sparkContext.emptyRDD[VertexId]
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

    case class NextHopsDist(nextHops: Set[VertexId], dist: Long) {
      def next: NextHopsDist = NextHopsDist(nextHops, dist + 1)

      def <(that: NextHopsDist): Boolean = dist < that.dist

      def min(that: NextHopsDist): NextHopsDist = if (dist < that.dist) this else that

      override def toString: String = "distance " + dist + ", parents " + nextHops
    }

    def allPairsShortestPaths(distFn: Edge[ED] => Double = _ => 1): RDD[(VertexId, Map[VertexId, NextHopsDist])] = {
      val initialMsg = Map(-1L -> NextHopsDist(Set.empty, -1L))
      val pregelg = g.mapVertices((vid, vd) => (vd, Map[VertexId, NextHopsDist](vid -> NextHopsDist(Set(vid), 0L)))).reverse.partitionBy(PartitionStrategy.EdgePartition2D, numPartitions=128)

      def vprog(v: VertexId, value: (VD, Map[VertexId, NextHopsDist]), message: Map[VertexId, NextHopsDist]): (VD, Map[VertexId, NextHopsDist]) = {
        val updatedValues = mergeMsg(value._2, message).filter(v => v._2.dist >= 0L)
        (value._1, updatedValues)
      }

      def sendMsg(triplet: EdgeTriplet[(VD, Map[VertexId, NextHopsDist]), ED]): Iterator[(VertexId, Map[VertexId, NextHopsDist])] = {
        val dstVertexId = triplet.dstId
        val srcMap = triplet.srcAttr._2
        val dstMap = triplet.dstAttr._2 // guaranteed to have dstVertexId as a key

        val updatesToSend: Map[VertexId, NextHopsDist] = srcMap.filter {
          case (vid, srcPD) => dstMap.get(vid) match {
            case Some(dstPD) =>
              dstPD.dist > srcPD.dist + 1 && !dstPD.nextHops.contains(triplet.srcId) // if it exists, is it a new, cheaper path?
            case _ => true // not found - new update
          }
        }.map(u => u._1 -> NextHopsDist(Set(triplet.srcId), u._2.dist + 1))

        if (updatesToSend.nonEmpty) Iterator[(VertexId, Map[VertexId, NextHopsDist])]((dstVertexId, updatesToSend))
        else Iterator.empty
      }

      def mergeMsg(m1: Map[VertexId, NextHopsDist], m2: Map[VertexId, NextHopsDist]): Map[VertexId, NextHopsDist] = {
        val m1k = m1.keySet
        val m2k = m2.keySet
        val allk = m1k.union(m2k)
        val merged: Map[VertexId, NextHopsDist] = allk.foldLeft(Map[VertexId, NextHopsDist]())((acc, k) => {
          val m1key = m1.get(k)
          val m2key = m2.get(k)
          (m1key, m2key) match {
            case (Some(m1nh), Some(m2nh)) => // vertex is in both
              val m1dist = m1nh.dist
              val m2dist = m2nh.dist
              if (m1dist == m2dist)       // same distances! merge the nexthopsdist
                acc.updated(k, NextHopsDist(m1nh.nextHops ++ m2nh.nextHops, m1dist))
              else if (m1dist < m2dist)
                acc.updated(k, m1nh)
              else
                acc.updated(k, m2nh)

            case (Some(m1nh), None) => acc.updated(k, m1nh)
            case (None, Some(m2nh)) => acc.updated(k, m2nh)
            case _  => acc    // we should never get here
          }
        })
        merged
      }

      val pregelRun = pregelg.pregel(initialMsg)(vprog, sendMsg, mergeMsg)
      val sps = pregelRun.vertices.map(v => v._1 -> v._2._2)

      sps
    }

    def buildShortestPaths: Map[VertexId, Map[VertexId, Set[List[VertexId]]]] = {
      // COLLECT HAPPENS HERE
      val sps = g.allPairsShortestPaths().collectAsMap.toMap
      sps.foldLeft(Map[VertexId, Map[VertexId, Set[List[VertexId]]]]())((acc, sp) => {
        val (src, allDsts) = sp
        def createPaths(vFrom: VertexId, vTo: VertexId): Set[List[VertexId]] = {
          def _createPaths(vFrom: VertexId, vTo: VertexId, currPaths: Set[List[VertexId]]): Set[List[VertexId]] = {
            if (vTo == vFrom) currPaths
            else {
              val nextHops = sps(vFrom)(vTo).nextHops
              nextHops.map(nh => _createPaths(nh, vTo, if (currPaths.isEmpty) Set(List(nh)) else currPaths.map(cp => cp :+ nh))).reduce(_ union _)
            }
          }
          _createPaths(vFrom, vTo, Set(List(vFrom)))
        }


        val allPathsOneSrc = allDsts.foldLeft(Map[VertexId, Set[List[VertexId]]]())((acc, dst) => {
          val spForSrc = createPaths(src, dst._1)
          acc.updated(dst._1, spForSrc)
        })
        acc.updated(sp._1, allPathsOneSrc)
      })
    }

    /**
      *
      * @param endpoints  true if endpoints should be included in centrality calculation; otherwise false. Default: false
      * @param normalize  true if centrality measures should be normalized by (nv-1)(nv-2); false otherwise. Default: true. Note:
      *                   normalization assumes that the graph is directed. For undirected normalization, divide centrality
      *                   calculations by 2.
      * @return           Map of vertexId to centrality measure.
      */
    def betweennessCentrality(endpoints:Boolean = false, normalize:Boolean = true): Map[VertexId, Double] = {
      val shortestPaths = g.buildShortestPaths
      val bcMap = scala.collection.mutable.Map[VertexId, Double]()
      g.vertices.collect.foreach(v => bcMap.update(v._1, 0.0))

      shortestPaths.foreach(pathsForVertex => {
        val (_, dsts) = pathsForVertex
        dsts.foreach(dstpaths => {
          val (_, paths) = dstpaths
          paths.foreach(path => {
            val pathToUse = if (endpoints) path else path.drop(1).dropRight(1)
            pathToUse.foreach(v => {

              bcMap(v) += 1.0 / paths.size
            })
          })
        })
      })

      val nv = g.vertices.count
      val scaleFactor = if (normalize) (nv - 1) * (nv - 2) else 1
      bcMap.toMap.map(l => l._1 -> l._2 / scaleFactor)
    }
  }

  object SmallGraphs {
    private val densityCutoff = 0.62
    private def makeNodesFrom(r:Seq[Long])(implicit sc: SparkContext): RDD[(VertexId, Unit)] = sc.parallelize(r.map(v => (v, ())))
    private def makeNodes(n:Long)(implicit sc: SparkContext) : RDD[(VertexId, Unit)] = makeNodesFrom(0L.until(n))

    private def makeEdgesFrom(s:Seq[(Long, Long)])(implicit sc: SparkContext): RDD[Edge[Unit]] =
      sc.parallelize(s.map(e => Edge(e._1, e._2, ())))
    /**
      * A directed cycle graph with a given number of nodes.
      * @param n    Number of nodes in the circle graph.
      * @return     A GraphX graph
      */
    def cycleDiGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
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
    def pathDiGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
      val r = 0L.until(n)
      val rLen = r.length - 1
      val nodes = makeNodes(n)
      val edges: RDD[Edge[Unit]] = sc.parallelize(0.until(rLen).map(i => Edge(r(i), r(i + 1), ())))
      Graph(nodes, edges)
    }

    /**
      * A directed wheel graph with a given number of nodes. VertexId 0 is the center node.
      * @param n  Number of nodes in the wheel graph, including the center node
      * @return   A GraphX graph
      */
    def wheelDiGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
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
    def houseDiGraph(implicit sc: SparkContext): Graph[Unit, Unit] = {
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
    def starDiGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
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
    def binaryTreeDiGraph(depth:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
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
    private def makePairs(n:Long, nv:Long, ordered: Boolean = false)(implicit sc: SparkContext): RDD[(Long, Long)] = {
      val nv2 = nv * nv
      val initialRDD = uniformRDD(sc, 20L.max(n * 2))
        .map(v => (nv2 * v).toLong).map(v => (v / nv, v % nv)).filter(p => p._1 != p._2)
        .map(p => if (ordered && (p._1 > p._2)) p.swap else p).distinct.truncate(n)

      assert(initialRDD.count == n, "Length mismatch: expected " + n + ", received " + initialRDD.count)
      initialRDD
    }
    /**
      * A directed graph of a given order and size, with randomly-generated edges.
      * Note: the graph will not contain self-loops.
      * @param nv   The number of vertices in the graph
      * @param ne   The number of random directed edges / arcs to include in the graph
      * @return     A GraphX graph
      */
    def randomDiGraph(nv:Long, ne:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
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
    def randomGraph(nv:Long, ne:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
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
      val uniG = Graph(nodes, edgeRDD)
      uniG.toUndirected
    }

    /**
      * Creates an RDD of pairs representing all (non-self-looped) edges for n vertices
      * @param n  Number of vertices
      * @return   an RDD of Pairs
      */
    private def allPairsRDD(n:Long)(implicit sc: SparkContext): RDD[(Long, Long)] = {
      val nRDD = sc.parallelize(0L.until(n))
      nRDD.cartesian(nRDD).filter(p => p._1 != p._2)
    }

    /**
      * An undirected complete graph (all pairs of nodes interconnected).
      * @param n  The number of vertices in the graph
      * @return   A GraphX graph
      */
    def completeGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = {
      val nodes = makeNodes(n)
      val edges = allPairsRDD(n).map(p => Edge(p._1, p._2, ()))

      Graph(nodes, edges)
    }

    /**
      * An undirected path graph of a given length.
      * @param n  Length of the path graph
      * @return   A GraphX graph
      */
    def pathGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = pathDiGraph(n).toUndirected

    /**
      * An undirected cycle graph with a given number of nodes.
      * @param n    Number of nodes in the graph
      * @return     A GraphX graph
      */
    def cycleGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = cycleDiGraph(n).toUndirected

    /**
      * An undirected wheel graph with a given number of nodes. VertexId 0 is the center node.
      * @param n    Number of nodes in the graph, including the center node
      * @return     A GraphX graph
      */
    def wheelGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = wheelDiGraph(n).toUndirected

    /**
      * An undirected house graph.
      * @return     A GraphX graph
      */
    def houseGraph(implicit sc: SparkContext): Graph[Unit, Unit] = houseDiGraph.toUndirected

    /**
      * An undirected star graph with a given number of nodes. VertexId 0 is the center node.
      * @param n    The number of nodes, including the center vertex
      * @return     A GraphX graph
      */
    def starGraph(n:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = starDiGraph(n).toUndirected

    /**
      * An undirected full binary tree of a given depth. VertexId 0 is the root node.
      * @param depth    The depth of the binary tree
      * @return         A GraphX graph
      */
    def binaryTreeGraph(depth:Long)(implicit sc: SparkContext): Graph[Unit, Unit] = binaryTreeDiGraph(depth).toUndirected
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("com").setLevel(Level.WARN)
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("bromberger").setLevel(Level.WARN)
    val conf = new SparkConf().setAppName("test").setMaster("local[*]")
    implicit val sc = new SparkContext(conf)
    val r = new scala.util.Random

    def runOneDiGraphTest(n:Int): Unit = {
      val nv = 3.max(r.nextInt(n))
      val ne = 2.max(r.nextInt(nv) * r.nextInt(nv))
      println("running digraph with (" + nv + ", " + ne + ")")
      val g = SmallGraphs.randomDiGraph(nv, ne)
      val vct = g.vertices.count()
      val ect = g.edges.count()
      assert(vct == nv, "vct " + vct + " != nv " + nv)
      assert(ect == ne, "ect " + ect + " != ne " + ne)
    }

    def runOneGraphTest(n:Int): Unit = {
      val nv = 3.max(r.nextInt(n))
      val ne = 2.max(r.nextInt(nv) * r.nextInt(nv) / 2)
      println("running graph with (" + nv + ", " + ne + ")")
      val g = SmallGraphs.randomGraph(nv, ne)
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
