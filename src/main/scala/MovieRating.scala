import java.nio.charset.CodingErrorAction

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.{Codec, Source}




object MovieRating extends java.io.Serializable{


  def movieIdAndName()={
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)
    var mappedMovie2 = List[(Int,String)]()
    val lines: Iterator[String] = Source.fromFile("D:\\datasets\\ml-100k\\u.item").getLines()
    for(line <- lines){
    var fields = line.split('|')
      if(fields.length >1) {
        mappedMovie2 :+= (fields(0).toInt , fields(1) )
      }
   }
     mappedMovie2;
  }


  def movieIdWithRating(line:String) = {
    val fields = line.split("\t")(1)
    (fields.toInt,1)
  }


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setMaster("local[*]").setAppName("MovieRating")
    val sc= new SparkContext(conf)
    val broadValues = sc.broadcast(Map(movieIdAndName: _*))

    val data: RDD[String] = sc.textFile("D:\\datasets\\ml-100k\\u.data")

    val  ratingCnt = data.map(movieIdWithRating).reduceByKey((x,y) => x+y)

    val res = ratingCnt.map(x => (broadValues.value(x._1),x._2))
    val result  = res.sortBy(_._2).collect
    result.foreach(println)

  }
}
