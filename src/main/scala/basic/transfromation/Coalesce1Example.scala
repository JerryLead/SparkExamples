package basic.transfromation

import org.apache.spark.sql.SparkSession

object Coalesce1Example {
  def main(args: Array[String]): Unit = {
    val spark  = SparkSession.builder.appName("coalesce example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext
    val inputRDD = sc.parallelize(1 to 10, 5)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val coalesceRDD = inputRDD.coalesce(3,true)
    println(coalesceRDD.toDebugString)
    println("------------result rdd----------")
    coalesceRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
    println(coalesceRDD.toDebugString)

  }
}
