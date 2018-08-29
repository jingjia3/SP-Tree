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
    QueryDecompose.decompositionToPath(broadcastBgp)
    val queryPaths: ArrayBuffer[ArrayBuffer[(String, Int, String)]] = QueryDecompose.getPath  
    val broadcastPaths: Broadcast[ArrayBuffer[ArrayBuffer[(String, Int, String)]]] = sc.broadcast(queryPaths)
    broadcastBgp.unpersist(true)

    //collect the share vertices in different paths
    var varMap:Map[String, (Int, Set[Int])] = Map()
    if(broadcastPaths.value.length == 1){
      val path = broadcastPaths.value(0)
      val num = path.length - 1
      varMap += (path(num)._3 -> (num, Set(0)))
    } else {
      for(i <- 0 until broadcastPaths.value.length){
        for(j <- 1 until broadcastPaths.value(i).length){
          val node = broadcastPaths.value(i)(j)._3
          if(varMap.containsKey(node)){
            var num: Int = varMap(node)._1
            var tmpS: Set[Int] = varMap(node)._2
            if(j > num){
              num = j
            }
            tmpS += i
            varMap.-(node)
            varMap += (node -> (num, tmpS))
          } else {
            varMap += (node -> (j, Set(i)))
          }
        }
      }
      varMap = varMap.filter(elem => elem._2._2.size >= 2)
    }
    val root = queryPaths(0)(queryPaths(0).length -1)._3
    val mergedNode = varMap
    val broadcastRoot: Broadcast[String] = sc.broadcast(root)
    val broadcastMergedNode: Broadcast[Map[String, (Int, Set[Int])]] = sc.broadcast(mergedNode)
    
    //collect the information of adjacent edges of vertices in query graph
    var vertexMap:Map[String, Set[(Int, String)]] = Map()
    for(ele <-  broadcastBgp.value){
      if(vertexMap.contains(ele._1)){
        val tmp = (0, ele._2)
        vertexMap(ele._1) += tmp
      }
      if(vertexMap.contains(ele._3)){
        val tmp = (1, ele._2)
        vertexMap(ele._3) += tmp
      }
      if(!vertexMap.contains(ele._1)){
        vertexMap += (ele._1 -> Set((0, ele._2)))
      }
      if(!vertexMap.contains(ele._3)){
        vertexMap += (ele._3 -> Set((1, ele._2)))
      }
    }
    val broadcastVertexMap: Broadcast[Map[String, Set[(Int, String)]]] = sc.broadcast(vertexMap)
    
    // start to handle the graph before pregel computing
    log.info("Started Graph Loading...") 
    val startLoading: Long = currentTimeMillis() 
    val lines: RDD[String] = sc.textFile(args(0))
    
    val splitted: RDD[(String, String, String)] = lines.distinct.filter(line => line.trim().startsWith("<"))
    .map(line => splitRDF(line)).filter(line => line != null && broadcastPredicates.value.contains(line._2))
    splitted.persist(StorageLevel.MEMORY_ONLY)
  
    val subjects: RDD[String] = splitted.map(triple => triple._1)
    val objects: RDD[String] = splitted.map(triple => triple._3)
    val distinctNodes: RDD[String] = sc.union(subjects, objects).distinct; 
    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId
    zippedNodes.persist(StorageLevel.MEMORY_ONLY)
    for(x<-zippedNodes)println(x)
    val splittedTriples: RDD[(String, (String, String))] = splitted.map(triple => (triple._3, (triple._1, triple._2)))
    splitted.unpersist(true)
    
    // o s p oid
    val joinedTriples: RDD[(String, ((String, String), Long))] = splittedTriples.join(zippedNodes)
    // s oid p
    val replacedObjects: RDD[(String, (Long, String))] = joinedTriples.map(line => (line._2._1._1, (line._2._2, line._2._1._2)));
    // s oid p sid
    val joinedSubjects: RDD[(String, ((Long, String), Long))] = replacedObjects.join(zippedNodes)

    var edges: RDD[Edge[String]] = joinedSubjects.map(line => Edge(line._2._2, line._2._1._1, line._2._1._2))
    
    val subPre: RDD[(Long, Set[(Int, String)])] = joinedSubjects.map(elem => (elem._2._2, Set((0, elem._2._1._2)))).distinct()
    val objPre: RDD[(Long, Set[(Int, String)])] = joinedSubjects.map(elem => (elem._2._1._1, Set((1, elem._2._1._2)))).distinct()
    val uniVertex: RDD[(Long, Set[(Int, String)])] = sc.union(subPre, objPre)
    val vertex: RDD[(Long, Set[(Int, String)])] = uniVertex.reduceByKey((a, b) => (a ++ b))
    val zVertices:RDD[(Long, String)] =zippedNodes.map(line=>(line._2,line._1))
    val addAttr: RDD[(Long, (String, Set[(Int, String)]))] = zVertices.join(vertex)   
    var vertices = addAttr.map(node => (node._1, (node._2._1, 0, ArrayBuffer[(Int, Map[String, String])](), ArrayBuffer[ArrayBuffer[Map[String, String]]](), node._2._2, 
        Map[(Int, String), ArrayBuffer[(Int, Map[String, String])]]()))) 

    var _originalGraph = Graph(vertices, edges).persist(StorageLevel.MEMORY_ONLY).partitionBy(PartitionStrategy.EdgePartition1D) 
    
    val numberV = _originalGraph.vertices.count()
    val numberE = _originalGraph.edges.count()
    log.info("The number of vertices: " + numberV + " The number of edges: " + numberE)
    
    val endLoading:Long = currentTimeMillis() - startLoading
    log.info("Finished Graph Loading in " + endLoading + " ms") 
    
    //initial messages
    val map: Map[String, String] = Map("#" -> "#")
    val initialMsg: (Int, ArrayBuffer[(Int, Map[String, String])]) = (0, ArrayBuffer((0, map)))
    
    //vertex compute
    //(label, superStep, matchS, pathS, shapeI, resultS)
    type VD = (String, Int, ArrayBuffer[(Int, Map[String, String])], ArrayBuffer[ArrayBuffer[Map[String, String]]], Set[(Int, String)], 
        Map[(Int, String), ArrayBuffer[(Int, Map[String, String])]])
    def vprog(id:VertexId, vd:VD, iMsg:(Int, ArrayBuffer[(Int, Map[String, String])])) = {
      
      val paths = broadcastPaths.value
      val mergedNode: Map[String, (Int, Set[Int])] = broadcastMergedNode.value
      val vertexMap: Map[String, Set[(Int, String)]] = broadcastVertexMap.value
      
      var ite = iMsg._1 //superstep number
      val stp = ite + 1
      //the set of messages to send in the next superstep
      var matchS: ArrayBuffer[(Int, Map[String, String])] = ArrayBuffer()
      // completed paths
      var completedPathS: ArrayBuffer[ArrayBuffer[Map[String, String]]] = vd._4
      //the set of vertices to merge in this superstep, not including root node
      var matchVetices: Set[String] = Set()
      //paths to be merged in the remaining paths
      var waitToMergedPath: Map[(Int, String), ArrayBuffer[(Int, Map[String, String])]] = vd._6

      if((ite == 0 && iMsg._2(0)._2.containsKey("#"))){
        for(i <- 0 until paths.length){
          val adjEdgeSet: Set[(Int, String)] = vertexMap(paths(i)(0)._3)
          if(adjEdgeSet.subsetOf(vd._5)){
            if(paths(i)(0)._3.startsWith("?")){
              val elem = (i, Map(paths(i)(0)._3 -> vd._1))
              matchS += elem
            } else if(paths(i)(0)._3.equals(vd._1)){
              val elem = (i, Map[String, String]())
              matchS += elem
            }
          }                        
        }
        if(matchS.length > 0){
          (vd._1, stp, matchS, vd._4, vd._5, vd._6)
        } else{
          (vd._1, ite, matchS, vd._4, vd._5, vd._6)
        }        
      } else {
        var toMergedPath: Map[Int, ArrayBuffer[(Int, Map[String, String])]] = Map()
        for(i <- 0 until iMsg._2.length){
          val id: Int = iMsg._2(i)._1
          var map: Map[String, String] = iMsg._2(i)._2
          
          if(mergedNode.containsKey(paths(id)(ite)._3)){
            //the vertex matched in this superstep is the merged vertex
            val maxIte: Int = mergedNode(paths(id)(ite)._3)._1
            if(ite < maxIte){
              if(paths(id)(ite)._3.startsWith("?")){
                map += (paths(id)(ite)._3 -> vd._1)
                val elem = (id, map)
                if(!paths(id)(ite)._3.equals(broadcastRoot.value)){
                  matchS += elem
                }                
                val keyV = (id, paths(id)(ite)._3)                
                if(waitToMergedPath.contains(keyV)){
                  waitToMergedPath(keyV) += elem
                } else{
                  waitToMergedPath += (keyV -> ArrayBuffer(elem))
                }                   
              } else if (paths(id)(ite)._3.equals(vd._1)){
                val elem = (id, map)
                if(!paths(id)(ite)._3.equals(broadcastRoot.value)){
                  matchS += elem
                } 
                val keyV = (id, paths(id)(ite)._3)
                if(waitToMergedPath.contains(keyV)){
                  waitToMergedPath(keyV) += elem
                } else{
                  waitToMergedPath += (keyV -> ArrayBuffer(elem))
                }                
              }  
            } else if(ite == maxIte){                    
              if(paths(id)(ite)._3.startsWith("?")){
                map += (paths(id)(ite)._3 -> vd._1)
                val elem = (id, map)
                if(toMergedPath.containsKey(id)){  
                  toMergedPath(id) += elem
                } else{
                  toMergedPath += (id -> ArrayBuffer(elem))
                }
                matchVetices += paths(id)(ite)._3
              } else if(paths(id)(ite)._3.equals(vd._1)){
                val elem = (id, map)                    
                if(toMergedPath.containsKey(id)){  
                  toMergedPath(id) += elem
                } else{
                  toMergedPath += (id -> ArrayBuffer(elem))
                }
                matchVetices += paths(id)(ite)._3
              }  
            }
          } else {
            if(paths(id)(ite)._3.startsWith("?")){
              var newMap = map ++ Map(paths(id)(ite)._3 -> vd._1)
              val elem = (id, newMap)                
              matchS += elem   
            } else if (paths(id)(ite)._3.equals(vd._1)){
              val elem = (id, map)
              matchS += elem
            }
          }       
        }
        
        for(node <- matchVetices){
          val numPath: Set[Int] = mergedNode(node)._2  
          var flag = true
          for(num <- numPath){
            if(!toMergedPath.contains(num)){
              val keyV = (num, node)
              if(waitToMergedPath.contains(keyV)){
                if(node.equals(broadcastRoot.value)){
                  toMergedPath += (num -> waitToMergedPath(keyV))
                }
                waitToMergedPath.-(keyV)
              } else {
                flag = false
              }          
            }      
          }
          if(flag){
            if(!node.equals(broadcastRoot.value)){
              toMergedPath.keys.foreach(key => {
                matchS = matchS ++ toMergedPath(key)
              })  
            } else{
              toMergedPath.keys.foreach(key => {
                for(j <- 0 until toMergedPath(key).length){
                  val a:(Int, Map[String, String]) = toMergedPath(key)(j)
                  if(completedPathS.length == 0){            
                    for(i <- 0 until paths.length){
                      completedPathS += ArrayBuffer[Map[String, String]]() 
                    }
                  }
                  completedPathS(a._1) += a._2
                }
              }) 
            }             
          }
        }
        (vd._1, stp, matchS, completedPathS, vd._5, waitToMergedPath)
      }      
    }
  
    //send message optimizate
    def sendMsg(triplet:EdgeTriplet[VD, String]) = {
      
      val srcMsg:ArrayBuffer[(Int, Map[String, String])] = ArrayBuffer()
      val dstMsg:ArrayBuffer[(Int, Map[String, String])] = ArrayBuffer()
            
      val paths = broadcastPaths.value
      val vertexMap: Map[String, Set[(Int, String)]] = broadcastVertexMap.value
      val srcI = triplet.srcAttr._2
      val dstI = triplet.dstAttr._2
      
      if(srcI == dstI){     
        for(i <- 0 until triplet.srcAttr._3.length){
          val id:Int = triplet.srcAttr._3(i)._1
          if((paths(id)(srcI)._2 == 0 && (paths(id)(srcI)._3.equals(triplet.dstAttr._1) || paths(id)(srcI)._3.startsWith("?")))){
            if((paths(id)(srcI)._1.equals(triplet.attr) || paths(id)(srcI)._1.startsWith("?"))){
              if(vertexMap(paths(id)(srcI)._3).subsetOf(triplet.dstAttr._5)){
                srcMsg += triplet.srcAttr._3(i)
              }              
            }
          }
        }  
  
        for(i <- 0 until triplet.dstAttr._3.length){
          val id:Int = triplet.dstAttr._3(i)._1    
          if((paths(id)(dstI)._2 == 1 && (paths(id)(dstI)._3.equals(triplet.srcAttr._1) || paths(id)(dstI)._3.startsWith("?")))){
            if((paths(id)(dstI)._1.equals(triplet.attr) || paths(id)(dstI)._1.startsWith("?"))){
              if(vertexMap(paths(id)(dstI)._3).subsetOf(triplet.srcAttr._5)){
                dstMsg += triplet.dstAttr._3(i)
              }              
            }
          }
        }   
      } else if(srcI > dstI){
       for(i <- 0 until triplet.srcAttr._3.length){
          val id:Int = triplet.srcAttr._3(i)._1
          if((paths(id)(srcI)._2 == 0 && (paths(id)(srcI)._3.equals(triplet.dstAttr._1) || paths(id)(srcI)._3.startsWith("?")))){
            if((paths(id)(srcI)._1.equals(triplet.attr) || paths(id)(srcI)._1.startsWith("?"))){
              if(vertexMap(paths(id)(srcI)._3).subsetOf(triplet.dstAttr._5)){
                srcMsg += triplet.srcAttr._3(i)
              }              
            }
          }
        }  
      } else{
        for(i <- 0 until triplet.dstAttr._3.length){
          val id:Int = triplet.dstAttr._3(i)._1    
          if((paths(id)(dstI)._2 == 1 && (paths(id)(dstI)._3.equals(triplet.srcAttr._1) || paths(id)(dstI)._3.startsWith("?")))){
            if((paths(id)(dstI)._1.equals(triplet.attr) || paths(id)(dstI)._1.startsWith("?"))){
              if(vertexMap(paths(id)(dstI)._3).subsetOf(triplet.srcAttr._5)){
                dstMsg += triplet.dstAttr._3(i)
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
    def mergeMsg(a:(Int, ArrayBuffer[(Int, Map[String, String])]), b:(Int, ArrayBuffer[(Int, Map[String, String])])) = {
      (a._1, (a._2 ++ b._2))     
    }
    
    // run query
    log.info("Started Query Running...")
    val startRunning:Long = currentTimeMillis()  
    val pregelGraph = _originalGraph.pregel(initialMsg, Int.MaxValue, EdgeDirection.Either)(vprog, sendMsg, mergeMsg)
    val endRunning:Long = System.currentTimeMillis() - startRunning
	  log.info("Finished Query Running in " + endRunning + " ms" )
    
	  val startResult:Long = currentTimeMillis()
    val finalResult: RDD[Map[String, String]] = pregelGraph.vertices.map(v => v._2._4).filter(v => v.length != 0 )
    .flatMap( v => {
      var resultArray:ArrayBuffer[Map[String, String]] = ArrayBuffer()
      for(j <- 0 until v(0).length){
        val map: Map[String, String] = v(0)(j)
          resultArray += map            
      }
      if(v.length > 1){
        for(i <- 1 until v.length){
          val tmpArray = resultArray
          resultArray = ArrayBuffer()
          for(j <- 0 until v(i).length){
            for(k <- 0 until tmpArray.length){
              if(compatible(tmpArray(k), v(i)(j))){
                val map = tmpArray(k) ++ v(i)(j)
                resultArray += map
              }
            }
          }      
        }
      }      
      resultArray
    })
    
    finalResult.persist(StorageLevel.MEMORY_ONLY)
    val result: Long = finalResult.count()
    
    _originalGraph.vertices.unpersist(true)
    _originalGraph.edges.unpersist(true)
    broadcastPaths.unpersist(true)
    broadcastVertexMap.unpersist(true)
    broadcastRoot.unpersist(true)
    broadcastMergedNode.unpersist(true)

	  //print result
    if(result == 0L){
	    log.info("There is no result for the query.")
	  } else {
	    log.info("There are " + result + " answers for the query.")
	    val ans = finalResult.distinct()
	    if(ans.count() <= 3L){
	      for(i <- 0 until ans.collect().length){
	        val map = ans.collect()(i)
	        log.info(map)
	      }	
	    } else {
	      println("We will print the first 10:")
	      for(i <- 0 until 3){
	        val map = ans.take(3)(i)
	        log.info(map)
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