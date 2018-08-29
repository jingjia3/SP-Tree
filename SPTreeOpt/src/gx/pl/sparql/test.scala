package gx.pl.sparql
import org.slf4j.Logger
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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.AccumulatorV2
import Array._
import org.apache.spark.graphx.util.GraphGenerators
import scala.collection.mutable.ArrayBuffer
//import scala.util.automata
import scala.util.matching.Regex
object test {
  def main(args:Array[String]){
    var finalResult=ArrayBuffer(("1","a",1),("2","a",2),("3",null,0),("1","a",3),("2","b",4),("3",null,0),("1","a",1),("2","a",2),("3",null,0))
    var countPath=ArrayBuffer[ArrayBuffer[(String,String,Int)]]()
    var countx=ArrayBuffer[(String,String,Int)]()
    
    for(x<-finalResult){
      if(x._3!=0)
        countx+=x
      else{
        countx+=x
        if(!countPath.contains(countx)){
          var clcountx=countx.clone()
          countPath+=clcountx
        }  
        countx.remove(0,countx.length)
      }
    }
    var countp=0
    for(x<-countPath){
      countp=countp+1
    }
    println("==========The numeber of all single path is "+countp+"============")
  }
}