package basic.transfromation

import org.apache.spark.sql.SparkSession

object CartesianExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("cartesian")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val inputRDD1 = sc.makeRDD(List((1,'a'), (2,'b'), (3,'c'), (4,'d')), 2)
    val inputRDD2 = sc.makeRDD(List((1,'A'), (2,'B')), 2)

    println("-------------input rdd1------------")
    inputRDD1.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    println("-------------input rdd2------------")
    inputRDD2.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD1.cartesian(inputRDD2)
    print(resultRDD.toDebugString)
    println("-------------result rdd------------")
    resultRDD.mapPartitionsWithIndex((pid, iter) => {
      iter.map(e => "PID = " + pid + ", value = " + e)
    }).foreach(println)
    //Thread.sleep(100000)
  }
}
