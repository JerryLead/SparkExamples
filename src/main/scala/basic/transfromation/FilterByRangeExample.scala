package basic.transfromation

import org.apache.spark.sql.SparkSession

object FilterByRangeExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("filter by range example").master("local[2]").getOrCreate()
    val sc = spark.sparkContext

    val data = Array[(Int, Char)](
      (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e'),
      (3,'f'),(2,'g'),(1,'h')
    )
    val inputRDD = sc.parallelize(data, 3).sortByKey()

    println("------------input rdd----------")
    inputRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)

    val resultRDD = inputRDD.filterByRange(2,5)
    println(resultRDD.toDebugString)
    println("------------result rdd----------")
    resultRDD.mapPartitionsWithIndex((pid, iter)=>{
      iter.map( value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
  }
}
