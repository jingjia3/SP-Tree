package gx.pl.sparql

import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.io.PrintWriter
import java.io.File
import org.slf4j.LoggerFactory
import java.lang.System.currentTimeMillis
import scala.collection.JavaConversions._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark._
import org.apache.spark.graphx._
import scala.collection.mutable.Set
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import Array._
import org.apache.spark.graphx.util.GraphGenerators
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.HashPartitioner


object QueryExecutor {
  
  def main(args:Array[String]){
    
    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)
    val log:Logger = Logger.getLogger("QueryExecutor.object")
    
    val conf = new SparkConf()
    conf.setAppName("SPARQL")
    var sc = new SparkContext(conf)
   
    //read sparql query
    val loadQuery = new QueryRead(args(1))
    val bgp:Set[(String,String,String)] = Set()
    val predicates:Set[String] = Set()
    log.info("Print out tps in BGP query: " + loadQuery.getBGP.size())
    for ( ele <- loadQuery.getBGP){
      val tp = (ele.getValue0, ele.getValue1, ele.getValue2)
      bgp += tp
      predicates += tp._2
      log.info("tp: " + tp)
    }
    val broadcastBgp: Broadcast[Set[(String, String, String)]] = sc.broadcast(bgp)
    val broadcastPredicates: Broadcast[Set[String]] = sc.broadcast(predicates)
    
    // decompose query
    QueryDecompose.decompositionToTree(broadcastBgp)
    val verticesMap: ArrayBuffer[String] = QueryDecompose.getVerticesMap
    val broadcastVerticesMap: Broadcast[ArrayBuffer[String]] = sc.broadcast(verticesMap)
    val father: ArrayBuffer[(String, Int, Int)] = QueryDecompose.getFather
    val broadcastFather: Broadcast[ArrayBuffer[(String, Int, Int)]] = sc.broadcast(father)
    val leaves: ArrayBuffer[Int] = QueryDecompose.getLeaves
    val broadcastLeaves: Broadcast[ArrayBuffer[Int]] = sc.broadcast(leaves)
    val mergedVertices: ArrayBuffer[(Boolean, Int, Set[Int])] = QueryDecompose.getMergedVertices
    val broadcastMergedVertices: Broadcast[ArrayBuffer[(Boolean, Int, Set[Int])]] = sc.broadcast(mergedVertices)
    broadcastBgp.unpersist(true)
    
    // start to handle the graph before pregel computing
    log.info("Started Graph Loading...") 
    val startLoading: Long = currentTimeMillis() 
    val lines: RDD[String] = sc.textFile(args(0))
    
    //val splitted: RDD[(String, String, String)] = lines.distinct.filter(line => line.trim().startsWith("<"))
    //.map(line => splitRDF(line)).filter(line => line != null)
    //splitted.persist(StorageLevel.MEMORY_ONLY)
    //基本想法加边过滤
    val splitted: RDD[(String, String, String)] = lines.distinct().filter(line => line.trim().startsWith("<"))
    .map(line => splitRDF(line)).filter(line => line != null && broadcastPredicates.value.contains(line._2))
    splitted.persist(StorageLevel.MEMORY_ONLY)
    log.info("The number of edges: " + splitted.count())
  
    val subjects: RDD[String] = splitted.map(triple => triple._1)
    val objects: RDD[String] = splitted.map(triple => triple._3)
    val distinctNodes: RDD[String] = sc.union(subjects, objects).distinct
    log.info("The number of vertices: " + distinctNodes.count())
    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId
    zippedNodes.persist(StorageLevel.MEMORY_ONLY)
    for(x<-zippedNodes)println(x)
    val splittedTriples: RDD[(String, (String, String))] = splitted.map(triple => (triple._3, (triple._1, triple._2)))
    splitted.unpersist(true)
    
    // o s p oid
    val joinedTriples: RDD[(String, ((String, String), Long))] = splittedTriples.join(zippedNodes)
    val overstring:RDD[(Long,String)]=joinedTriples.map(x=>(x._2._2,x._1)).distinct()
    // s oid p
    val replacedObjects: RDD[(String, (Long, String))] = joinedTriples.map(line => (line._2._1._1, (line._2._2, line._2._1._2)));
    // s oid p sid
    val joinedSubjects: RDD[(String, ((Long, String), Long))] = replacedObjects.join(zippedNodes)

    var edges: RDD[Edge[String]] = joinedSubjects.map(line => Edge(line._2._2, line._2._1._1, line._2._1._2))
    
    //val subPre: RDD[(Long, Set[(Int, String)])] = joinedSubjects.map(elem => (elem._2._2, Set((0, elem._2._1._2)))).distinct()
    //val objPre: RDD[(Long, Set[(Int, String)])] = joinedSubjects.map(elem => (elem._2._1._1, Set((1, elem._2._1._2)))).distinct()
    //val uniVertex: RDD[(Long, Set[(Int, String)])] = sc.union(subPre, objPre)
    //val vertex: RDD[(Long, Set[(Int, String)])] = uniVertex.reduceByKey((a, b) => (a ++ b))
    val zVertices:RDD[(Long, String)] =zippedNodes.map(line=>(line._2,line._1))
    //val addAttr: RDD[(Long, (String, Set[(Int, String)]))] = zVertices.join(vertex)   
    
    //(label，（上一个点的标号，匹配集合），待合并点的从点标号到匹配集合的映射，答案映射)
    var vertices = zVertices.map(node => (node._1, (node._2, new Array[ArrayBuffer[Map[String, String]]](broadcastVerticesMap.value.length), Map[Int, ArrayBuffer[Map[String, String]]](),
        ArrayBuffer[Map[String, String]](), 0))) 
 
    var _originalGraph = Graph(vertices, edges).partitionBy(PartitionStrategy.EdgePartition1D)
    val numberV = _originalGraph.vertices.count()
    val numberE = _originalGraph.edges.count()
    
    val endLoading:Long = currentTimeMillis() - startLoading
    log.info("Finished Graph Loading in " + endLoading + " ms") 
    
    //val filterGraph = _originalGraph.subgraph(e => broadcastPredicates.value.contains(e.attr))
    //val n = filterGraph.vertices.count()
    //val m = filterGraph.edges.count()
    
    //initial messages
    val map: Map[String, String] = Map("#" -> "#")
    val initialMsg:(Int, ArrayBuffer[(Int, ArrayBuffer[Map[String, String]])]) = (0,ArrayBuffer((-1, ArrayBuffer(map))))
    
    //vertex compute
    type VD = (String, Array[ArrayBuffer[Map[String, String]]], Map[Int, ArrayBuffer[Map[String, String]]], ArrayBuffer[Map[String, String]], Int)
    def vprog(id:VertexId, vd:VD, iMsg:(Int, ArrayBuffer[(Int, ArrayBuffer[Map[String, String]])])) = {
      
      val verticesMap: ArrayBuffer[String] = broadcastVerticesMap.value
      val father: ArrayBuffer[(String, Int, Int)] = broadcastFather.value
      val leaves: ArrayBuffer[Int] = broadcastLeaves.value
      val mergedVertices: ArrayBuffer[(Boolean, Int, Set[Int])] = broadcastMergedVertices.value
      
      var matchs = new Array[ArrayBuffer[Map[String, String]]](broadcastVerticesMap.value.length)
      for(i <- 0 until matchs.length) {
            matchs(i) = ArrayBuffer[Map[String, String]]() //初始化
      }
      
      var waitToMerged: Map[Int, ArrayBuffer[Map[String, String]]] = vd._3
      var results = vd._4
      var toMergedVertices:Set[Int] = Set() //本轮该合并的点
      
      val ite = iMsg._1 + 1
      
      if(iMsg._2.length == 1 && iMsg._2(0)._2(0).containsKey("#")){
        //第0个超步,匹配叶子结点
        for(i <- 0 until leaves.length){
          val spt = leaves(i) //spt，该叶子点的标号
          val variableV: String = verticesMap(spt)
          if(variableV.startsWith("?")){
            matchs(spt) += Map(verticesMap(spt) -> vd._1)
          } else if(variableV.equals(vd._1)){
            matchs(spt) += Map[String,String]()
          }          
        }        
      } else {
        //其他超步
        for(i <- 0 until iMsg._2.length){
          val id: Int = iMsg._2(i)._1 //上一轮匹配的顶点标号
          val pMatchs: ArrayBuffer[Map[String, String]] = iMsg._2(i)._2 //局部匹配集合
          val spt = father(id)._3 //本轮匹配点的标号
          
          if(!mergedVertices(spt)._1){
            //如果本轮匹配的点不是合并点
            if(father(spt)._3 == -1){
              //是根节点，一条三元组
              if(verticesMap(spt).startsWith("?")){
                for(j <- 0 until pMatchs.length){
                  var result = pMatchs(j)
                  result += (verticesMap(spt) -> vd._1)
                  results += result
                }
              } else if(verticesMap(spt).equals(vd._1)){
                results = results ++ pMatchs
              }
            } else {
              //中间节点，非根节点
              if(verticesMap(spt).startsWith("?")){
                for(j <- 0 until pMatchs.length){
                  var pMatch = pMatchs(j)
                  pMatch += (verticesMap(spt) -> vd._1)
                  matchs(spt) += pMatch
                }
              } else if(verticesMap(spt).equals(vd._1)){
                matchs(spt) = matchs(spt) ++ pMatchs
              }
            }
          } else {
            //是合并点
            var toMergedFlag = true
            if(verticesMap(spt).startsWith("?")){ 
              for(j <- 0 until pMatchs.length){
                var pMatch = pMatchs(j)
                pMatch += (verticesMap(spt) -> vd._1)
                if(waitToMerged.contains(id)){
                  waitToMerged(id) += pMatch
                } else{
                  waitToMerged += (id -> ArrayBuffer(pMatch))
                }                
              }
            } else if(verticesMap(spt).equals(vd._1)){
              for(j <- 0 until pMatchs.length){
                if(waitToMerged.contains(id)){
                  waitToMerged(id) += pMatchs(j)
                } else{
                  waitToMerged += (id -> ArrayBuffer(pMatchs(j)))
                }                
              }
            } else {
              //spt匹配不到该点
              toMergedFlag = false
            }
            if(toMergedFlag && mergedVertices(spt)._2 == id){
              toMergedVertices += spt
            }
          }         
        } //end for
        
        for(mergedVId <- toMergedVertices){
          var mapArr:ArrayBuffer[Map[String, String]] = ArrayBuffer()
          val verSet: Set[Int] = mergedVertices(mergedVId)._3//合并merged点，需要哪些孩子节点
          val lastVId = mergedVertices(mergedVId)._2
          mapArr = waitToMerged(lastVId)
          verSet.-(lastVId)
          waitToMerged.-(lastVId)
          var flag = true
          for(v <- verSet){
            if(!waitToMerged.contains(v)){
               flag = false
            }
          }
          if(flag){
            for(v <- verSet){
              val tmpArr = mapArr
              mapArr = ArrayBuffer()
              if(waitToMerged.contains(v)){
                var hadMatchs: ArrayBuffer[Map[String, String]] = waitToMerged(v)
                waitToMerged.-(v)
                for(j <- 0 until hadMatchs.length){
                  for(k <- 0 until tmpArr.length){
                    if(compatible(tmpArr(k), hadMatchs(j))){
                      val map = tmpArr(k) ++ hadMatchs(j)
                      mapArr += map
                    }
                  }
                }
              }
            }
          } else{
            for(v <- verSet){
              if(waitToMerged.contains(v)){
                 waitToMerged.-(v)
              }
            }
            mapArr = ArrayBuffer()
          }
          if(!mapArr.isEmpty){
            if(father(mergedVId)._3 == -1){
              //合并点为根节点
              results = results ++ mapArr
              matchs = Array()
              waitToMerged = Map()
            } else {
              matchs(mergedVId) = matchs(mergedVId) ++ mapArr
            }
          }          
        }        
      }
      (vd._1, matchs, waitToMerged, results, ite)      
    }
  
    //send message
    def sendMsg(triplet:EdgeTriplet[VD, String]) = {
      
      //（点的标号，匹配到该点的顶点映射集合）
      val srcMsg:ArrayBuffer[(Int, ArrayBuffer[Map[String, String]])] = ArrayBuffer()
      val dstMsg:ArrayBuffer[(Int, ArrayBuffer[Map[String, String]])] = ArrayBuffer()
      val verticesMap: ArrayBuffer[String] = broadcastVerticesMap.value
      val father: ArrayBuffer[(String, Int, Int)] = broadcastFather.value
      
      val srcI = triplet.srcAttr._5
      val dstI = triplet.dstAttr._5
      
      if(srcI == dstI){
        for(i <- 0 until triplet.srcAttr._2.length){
          val mapArr: ArrayBuffer[Map[String, String]] = triplet.srcAttr._2(i)
          if(!mapArr.isEmpty){
            if(father(i)._2 == 0 && (father(i)._1.equals(triplet.attr) || father(i)._1.startsWith("?"))){
              val nextId = father(i)._3
              if(verticesMap(nextId).equals(triplet.dstAttr._1) || verticesMap(nextId).startsWith("?")){
                val elem = (i, mapArr)
                srcMsg += elem
              }
            }
          }
        }
        
        for(i <- 0 until triplet.dstAttr._2.length){
          val mapArr = triplet.dstAttr._2(i)
          if(!mapArr.isEmpty){
            if(father(i)._2 == 1 && (father(i)._1.equals(triplet.attr) || father(i)._1.startsWith("?"))){
              val nextId = father(i)._3
              if(verticesMap(nextId).equals(triplet.srcAttr._1) || verticesMap(nextId).startsWith("?")){
                val elem = (i, mapArr)
                dstMsg += elem
              }
            }
          }
        }  
      } else if(srcI > dstI){
        for(i <- 0 until triplet.srcAttr._2.length){
          val mapArr: ArrayBuffer[Map[String, String]] = triplet.srcAttr._2(i)
          if(!mapArr.isEmpty){
            if(father(i)._2 == 0 && (father(i)._1.equals(triplet.attr) || father(i)._1.startsWith("?"))){
              val nextId = father(i)._3
              if(verticesMap(nextId).equals(triplet.dstAttr._1) || verticesMap(nextId).startsWith("?")){
                val elem = (i, mapArr)
                srcMsg += elem
              }
            }
          }
        }
      } else {
        for(i <- 0 until triplet.dstAttr._2.length){
          val mapArr = triplet.dstAttr._2(i)
          if(!mapArr.isEmpty){
            if(father(i)._2 == 1 && (father(i)._1.equals(triplet.attr) || father(i)._1.startsWith("?"))){
              val nextId = father(i)._3
              if(verticesMap(nextId).equals(triplet.srcAttr._1) || verticesMap(nextId).startsWith("?")){
                val elem = (i, mapArr)
                dstMsg += elem
              }
            }
          }
        }  
      }
      
      if((!srcMsg.isEmpty && !dstMsg.isEmpty)){
        Iterator((triplet.srcId, (dstI,dstMsg)),(triplet.dstId, (srcI,srcMsg)))
      } else if((!srcMsg.isEmpty && dstMsg.isEmpty)){
        Iterator((triplet.dstId, (srcI,srcMsg)))
      } else if((srcMsg.isEmpty && !dstMsg.isEmpty)){
        Iterator((triplet.srcId, (dstI,dstMsg)))
      }else {
        Iterator.empty
      }    
    }
    
    //merge message
    def mergeMsg(a:(Int, ArrayBuffer[(Int, ArrayBuffer[Map[String, String]])]), b:(Int, ArrayBuffer[(Int, ArrayBuffer[Map[String, String]])])) = {
      (a._1, (a._2 ++ b._2))   
    }
    
    // run query
    log.info("Started Query Running...")
    val startRunning:Long = currentTimeMillis()   
    val pregelGraph = _originalGraph.pregel(initialMsg, Int.MaxValue, EdgeDirection.Either)(vprog, sendMsg, mergeMsg)
    val endRunning:Long = System.currentTimeMillis() - startRunning
	  log.info("Finished Query Running in " + endRunning + " ms" )
    
	  val startResult:Long = currentTimeMillis()
    val finalResult: RDD[Map[String, String]] = pregelGraph.vertices.flatMap( v => v._2._4)
    
    finalResult.persist(StorageLevel.MEMORY_ONLY)
    log.info("The number of finalResult: " + finalResult.count())
    val result: Long = finalResult.count()
    
    broadcastVerticesMap.unpersist(true)
    broadcastFather.unpersist(true)
    broadcastLeaves.unpersist(true)
    broadcastMergedVertices.unpersist(true)
    
	  //print result
    if(result == 0L){
	    log.info("There is no result for the query.")
	  } else {
	    log.info("There are " + result + " answers for the query.")
	    val ans = finalResult.distinct()
	    if(ans.count() <= 3L){
	      for(i <- 0 until ans.collect().length){
	        val map = ans.collect()(i).filter(x => x._1.startsWith("?"))
	        println(map)
	      }	
	    } else {
	      println("We will print the first 10:")
	      for(i <- 0 until 3){
	        val map = ans.take(3)(i).filter(x => x._1.startsWith("?"))
	        println(map)
	      }
	    }
	  }
    finalResult.unpersist(true)
    val endResult:Long = System.currentTimeMillis() - startResult
	  log.info("Collected results in " + endResult + " ms" )
	  
    sc.stop()
    
  }
  
  def splitRDF(toSplite: String): (String, String, String) = {    
      // Remove leading and trailing whitespaces
      var line = toSplite.trim()
      var splited: Array[String] = line.split(" ")
      var tuple: Array[String] = new Array[String](3)
      if(splited.length == 4){
        for(i <- 0 until splited.length - 1){
          tuple(i) = splited(i).substring(1, splited(i).length()-1)
        }
      } else {
        tuple(0) = splited(0).substring(1, splited(0).length()-1)
        tuple(1) = splited(1).substring(1, splited(1).length()-1)
        var tuple2 = splited(2).substring(1, splited(2).length())
        for(i <- 3 until splited.length - 1){
          tuple2 += " "
          tuple2 += splited(i)
        }
        tuple(2) = tuple2.substring(0, tuple2.length()-1)
      }
      (tuple(0),tuple(1),tuple(2))           
  }
  
  def compatible(a:Map[String, String], b:Map[String, String]):Boolean = {      
      var flag = true
      b.keys.foreach(i => {
        if(a.contains(i) && !a(i).equals(b(i))){
          flag = false
        }
      })
      flag      
  }

}