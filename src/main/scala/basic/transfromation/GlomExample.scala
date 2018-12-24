package basic.transfromation

import org.apache.spark.sql.SparkSession

object GlomExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("filter example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val inputRDD = sc.makeRDD(1 to 10,3)

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.glom()
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value.toList)
    }).foreach(println)
  }

}
