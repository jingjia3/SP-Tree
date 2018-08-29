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
  
  private var keyArray: ArrayBuffer[Set[String]] = null
   
  def getPath = {
    path
  }
  
  def getKeyArray = {
    keyArray
  }
    
  def decompositionToPath(broadcastBgp: Broadcast[Set[(String, String, String)]]) {

        val triple: Set[(String, String, String)] = broadcastBgp.value
        var vmap: Map[String, Int] = Map() //顶点字符串映射到数字
        var rvmap: ArrayBuffer[String] = ArrayBuffer() //顶点反映射，数字到字符串
        var vset: Set[String] = Set() //顶点集合
        var vcnt: Int = 0 //顶点编号

        var remap: ArrayBuffer[String] = ArrayBuffer() //边反映射
        var ecnt: Int = 0 //边编号

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

        var graph: Array[ArrayBuffer[(Int, Int, Int)]] = new Array[ArrayBuffer[(Int, Int, Int)]](vcnt) //邻接链表存储查询图
        for(i <- 0 until graph.length) {
            graph(i) = ArrayBuffer[(Int, Int, Int)]() //初始化
        }
        for(i <- triple) {
            remap += i._2
            var tmp: (Int, Int, Int) = (ecnt, 0, vmap(i._3))
            graph(vmap(i._1)) += tmp //添加正向边
            tmp = (ecnt, 1, vmap(i._1))
            graph(vmap(i._3)) += tmp //添加反向边
            ecnt += 1
        }

        var que: Queue[Int] = Queue()
        que.enqueue(0)

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
            tree += ArrayBuffer[(Int, Int, Int)]()
        }

        var end: Int = 0 //标记生成路径中最长路径的叶子节点
        
        while(que.nonEmpty) {
            val head: Int = que.dequeue
            for(i <- graph(head)) {
                if(!eflag(i._1)) {
                    if(vflag(i._3)) {
                        end = vcnt
                        tree += ArrayBuffer[(Int, Int, Int)]()
                        var tmp: (Int, Int, Int) = (i._1, i._2, vcnt)
                        tree(head) += tmp
                        tmp = (i._1, i._2 ^ 1, head)
                        tree(vcnt) += tmp
                        rvmap+= rvmap(i._3)
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

        var sque: Queue[(Int, Int)] = Queue() //(id, deep)
        var from: Array[Int] = new Array[Int](vcnt)
        var send: (Int, Int) = (end, 0)
        vflag = new Array[Boolean](vcnt)
        for ( i <- 0 until vflag.length) {
            vflag(i) = false
        }

        vflag(end) = true
        from(end) = -1
        sque.enqueue((end, 0))

        //第二遍遍历找最远端
        while(sque.nonEmpty) {
            val head = sque.dequeue
            send = head
            for(i <- tree(head._1)) {
                if(!vflag(i._3)) {
                    from(i._3) = head._1
                    vflag(i._3) = true
                    sque.enqueue((i._3, head._2 + 1))
                }
            }
        }

        end = send._1
        
        //取第二次遍历最长路径的中点，即为所选根节点
        for(i <- 0 until send._2 / 2) {
            end = from(end)
        }

        path = ArrayBuffer()
        var pcnt: Int = 0

        var father: Array[(Int, Int, Int)] = new Array[(Int, Int, Int)](vcnt)
        que.clear()
        vflag = new Array[Boolean](vcnt)
        for ( i <- 0 until vflag.length) {
            vflag(i) = false
        }

        que.enqueue(end)
        vflag(end) = true
        father(end) = (0, 0, -1)

        //遍历生成路径
        while(que.nonEmpty) {
            var head = que.dequeue
            var flag: Boolean = false
            for(i <- tree(head)) {
                if(!vflag(i._3)) {
                    father(i._3) = (i._1, i._2 ^ 1, head)
                    que.enqueue(i._3)
                    vflag(i._3) = true
                    flag = true
                }
            }
            if(!flag) {
                path += ArrayBuffer[(String, Int, String)]()
                var tmp: (String, Int, String) = ("", -1, rvmap(head))
                path(pcnt) += tmp
                while(father(head)._3 != -1) {
                    tmp = (remap(father(head)._1), father(head)._2, rvmap(father(head)._3))
                    path(pcnt) += tmp
                    head = father(head)._3
                }
                pcnt += 1
            }
        }
        
        keyArray = ArrayBuffer()
        var verS: Set[String] = Set()
        for(i <- 0 until path.length){          
          var iS:Set[String] = Set()
          if(i == 0){
            for(j <- 0 until path(0).length){
              verS += path(0)(j)._3
            }
          } else{
            for(j <- 0 until path(i).length){
              iS += path(i)(j)._3
            }
            keyArray += verS.&(iS)
            verS = verS ++ iS
          }
        }
    }  
}