package basic.transfromation

import org.apache.spark.sql.SparkSession

object MapPartitionExample {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder.appName("map patition example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.parallelize(List(1,2,4,5,6,2,4), 2)

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.mapPartitions(iter => {
      var result = List[Int]()
      var i = 0
      while(iter.hasNext){
        // demo1
        i += iter.next
        // demo2
//        i += 1
//        result = result.::(iter.next()+5)
        //demo3
//        result = result :+ iter.next +2
        // demo4
//        result = (iter.next +2) +: result
      }
  //demo1
      result.::(i).iterator
  //demo 2-4
//      println(result)
//      result.iterator
    })
    println("-------------result rdd------------")
    println(resultRDD.toDebugString)
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
