package basic.transfromation

import org.apache.spark.sql.SparkSession

object MapExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("map example")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

//    val data = Array[(Int, Char)](
//      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),
//      (3,'f'),(2,'g'),(1,'h')
//    )
//    val inputRDD = sc.parallelize(data, 3)
//
//    println("-------------input rdd------------")
//    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)
//
////    val resultRDD = inputRDD.map(obj=>("key:"+obj._1, "val:"+obj._2))
//    val resultRDD = inputRDD.map(obj=>"key:"+obj._1)
//
//    println(resultRDD.toDebugString)
//    println("-------------result rdd------------")
//    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value)
//    }).foreach(println)

    val inputRDD = sc.parallelize(List("ubuntu linux", "windows10", "centos linux"))

    println("-------------input rdd------------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

//    val resultRDD = inputRDD.map(obj=>obj.split(" "))
//    val resultRDD = inputRDD.map(_.split(" "))
    val resultRDD = inputRDD.map(_.length)
    println(resultRDD.toDebugString)
    println("-------------result rdd------------")
    val resultRDDMap = resultRDD.mapPartitionsWithIndex((pid, iter)=>{
//      iter.map( value => "PID: " + pid + ", value: " + value.toList.toString())
      iter.map( value => "PID: " + pid + ", value: " + value)
    })
    resultRDDMap.foreach(println)
    println(resultRDDMap.toDebugString)
  }

}
