package gx.pl.sparql

import org.apache.spark.graphx._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import com.hp.hpl.jena.shared.PrefixMapping
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.collection.mutable.Set
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable._
import Array._
import org.apache.spark.broadcast.Broadcast

object QueryDecompose {
  
  private var path: ArrayBuffer[ArrayBuffer[(String, Int, String)]] = null
  
  private var queryTree: ArrayBuffer[ArrayBuffer[(String, Int)]] = null
  
  private var verticesMap: ArrayBuffer[String] = null
  
  private var father: ArrayBuffer[(String, Int, Int)] = null
  
  private var leaves: ArrayBuffer[Int] = null
  
  private var mergedVertices: ArrayBuffer[(Boolean, Int, Set[Int])] = null
  
  private var superstep: Int = 0
   
  def getPath = {
    path
  }
  
  def getTree = {
    queryTree
  }
  
  def getVerticesMap = {
    verticesMap
  }
  
  def getFather = {
    father
  }
  
  def getLeaves = {
    leaves
  }
  
  def getMergedVertices = {
    mergedVertices
  }
  
  def getSuperstep = {
    superstep
  }

  def decompositionToTree(broadcastBgp: Broadcast[Set[(String, String, String)]]) {
        val triple: Set[(String, String, String)] = broadcastBgp.value
        
        var vmap: Map[String, Int] = Map() //顶点字符串映射到数字
        var rvmap: ArrayBuffer[String] = ArrayBuffer() //顶点反映射，数字到字符串
        var vset: Set[String] = Set() //顶点集合
        var vcnt: Int = 0 //顶点个数，无重复
        for(i <- triple) {
            if(!vset.contains(i._1)) {
                vmap += (i._1 -> vcnt)
                rvmap+= i._1
                vset += i._1
                vcnt += 1
            }
            if(!vset.contains(i._3)) {
                vmap += (i._3 -> vcnt)
                rvmap+= i._3
                vset += i._3
                vcnt += 1
            }
        }
        
        var remap: ArrayBuffer[String] = ArrayBuffer() //边反映射，按照查询三元组遍历顺序编号
        var ecnt: Int = 0 //边编号
        var graph: Array[ArrayBuffer[(Int, Int, Int)]] = new Array[ArrayBuffer[(Int, Int, Int)]](vcnt) //邻接链表存储查询图（边的序号，边的方向，点的序号）
        for(i <- 0 until graph.length) {
            graph(i) = ArrayBuffer[(Int, Int, Int)]() //初始化
        }
        for(i <- triple) {
            remap += i._2 //边的反映射
            var tmp: (Int, Int, Int) = (ecnt, 0, vmap(i._3)) //（边的序号，边方向，点的标号）
            graph(vmap(i._1)) += tmp //添加正向边
            tmp = (ecnt, 1, vmap(i._1))
            graph(vmap(i._3)) += tmp //添加反向边
            ecnt += 1
        }

        var que: Queue[Int] = Queue()
        que.enqueue(0)//为队列添加第一个元素，队列中为顶点标号

        var vflag: Array[Boolean] = new Array[Boolean](vcnt) //标记点是否被遍历过
        for ( i <- 0 until vflag.length) {
            vflag(i) = false
        }
        vflag(0) = true
        
        var eflag: Array[Boolean] = new Array[Boolean](ecnt) //标记边是否被遍历过
        for ( i <- 0 until eflag.length) {
            eflag(i) = false
        }

        var tree: ArrayBuffer[ArrayBuffer[(Int, Int, Int)]] = ArrayBuffer() //存储图的生成树，若顶点已经遍历过，生成新的点。
        for(i <- 0 until vcnt) {
            tree += ArrayBuffer[(Int, Int, Int)]() //初始化为vcnt个点，若含有环，增加顶点
        }

        var end: Int = 0 //标记生成路径中最长路径的叶子节点
        
        //第一次遍历查询图，生成查询树 
        while(que.nonEmpty) {//宽度优先搜索
            val head: Int = que.dequeue
            for(i <- graph(head)) {
                if(!eflag(i._1)) {//边没有遍历过
                    if(vflag(i._3)) {//点已经遍历过
                        end = vcnt
                        tree += ArrayBuffer[(Int, Int, Int)]()//增加一个点
                        var tmp: (Int, Int, Int) = (i._1, i._2, vcnt) //正向
                        tree(head) += tmp
                        tmp = (i._1, i._2 ^ 1, head) //反向
                        tree(vcnt) += tmp
                        rvmap+= rvmap(i._3)//顶点个数加1
                        vcnt += 1
                    }
                    else {
                        end = i._3
                        var tmp: (Int, Int, Int) = (i._1, i._2, i._3)
                        tree(head) += tmp
                        tmp = (i._1, i._2 ^ 1, head)
                        tree(i._3) += tmp
                        vflag(i._3) = true
                        que.enqueue(i._3)
                    }
                    eflag(i._1) = true
                }
            }
        }

        verticesMap = rvmap
        var sque: Queue[(Int, Int)] = Queue() //(id, deep)
        var from: Array[Int] = new Array[Int](vcnt) //节点的父亲节点
        var send: (Int, Int) = (end, 0) //从第一遍生成树的最后一个点开始遍历
        vflag = new Array[Boolean](vcnt)//初始化，未遍历
        for ( i <- 0 until vflag.length) {
            vflag(i) = false
        }

        vflag(end) = true
        from(end) = -1//end为root点没有父亲节点
        sque.enqueue((end, 0))

        //第二遍遍历找最远端
        while(sque.nonEmpty) {
            val head: (Int, Int) = sque.dequeue
            send = head
            for(i <- tree(head._1)) {
                if(!vflag(i._3)) {
                    from(i._3) = head._1
                    vflag(i._3) = true
                    sque.enqueue((i._3, head._2 + 1))
                }
            }
        }

        end = send._1//第二遍遍历最远点的标号
        
        //取第二次遍历最长路径的中点，即为所选根节点
        for(i <- 0 until send._2 / 2) {
            end = from(end)
        }//此时end为所选根节点的标号

        path = ArrayBuffer()
        var pcnt: Int = 0
        
        queryTree = ArrayBuffer()//矩阵存储树
        father = ArrayBuffer()
        mergedVertices = ArrayBuffer()
        leaves = ArrayBuffer()
        for(i <- 0 until vcnt){
          queryTree += ArrayBuffer[(String, Int)]()
          for(j <- 0 until vcnt){
            //初始化矩阵，都不可达，有向边，只存自底向上遍历树，
            val elem = ("", -1)
            queryTree(i) += elem
          }
          val elem = ("", -1, -1)
          father += elem //初始化father
          val tmp: (Boolean, Int, Set[Int]) = (false, -1, Set[Int]())
          mergedVertices += tmp //初始化
        }

        que.clear()
        vflag = new Array[Boolean](vcnt)
        for ( i <- 0 until vflag.length) {
            vflag(i) = false
        }

        que.enqueue(end)
        vflag(end) = true
        
        //遍历生成路径
        while(que.nonEmpty) {
            var head = que.dequeue
            var flag: Boolean = false
            for(i <- tree(head)) {
                if(!vflag(i._3)) {
                    father(i._3) = (remap(i._1), i._2 ^ 1, head)
                    queryTree(i._3)(head) = (remap(i._1), i._2 ^ 1)
                    que.enqueue(i._3)
                    vflag(i._3) = true
                    flag = true
                }
            }
            if(!flag){
              leaves += head
              path += ArrayBuffer[(String, Int, String)]()
              var tmp: (String, Int, String) = ("", -1, rvmap(head))
              path(pcnt) += tmp
              while(father(head)._3 != -1) {
                  tmp = (father(head)._1, father(head)._2, rvmap(father(head)._3))
                  path(pcnt) += tmp
                  head = father(head)._3
              }
              pcnt += 1
            }
        }
        
        for(i <- 0 until pcnt){
          if(superstep < path(i).length){
            superstep = path(i).length
          }
        }
        
        que.clear()
        for(i <- 0 until leaves.length){
          que.enqueue(leaves(i))
        }
        while(que.nonEmpty){
          var head = que.dequeue()
          var id: Int = father(head)._3 //父亲点的标号
          if(id != -1){
            val nodeSet = mergedVertices(id)._3
            if(nodeSet.isEmpty){
              nodeSet += head
              mergedVertices(id) = (mergedVertices(id)._1, head, nodeSet)
            } else {
              nodeSet += head
              mergedVertices(id) = (true, head, nodeSet)
            }
            que.enqueue(id)
          }          
        }
    }  
}