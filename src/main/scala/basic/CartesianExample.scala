package basic


import org.apache.spark.sql.SparkSession

object CartesianExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Cartesian")
      .master("local[2]")
      .getOrCreate()

    val sc = spark.sparkContext

    val x = sc.makeRDD(List((1,'a'), (2,'b'), (3,'c'), (4,'d')), 2)
    val y = sc.makeRDD(List((1,'A'), (2,'B')), 2)

    //    println("Elements in x")
    //    x.mapPartitionsWithIndex((pid, iter) => {
    //      iter.map(e => "PID = " + pid + ", value = " + e)
    //    }).foreach(println)

    //    println("Elements in y")
    //    y.mapPartitionsWithIndex((pid, iter) => {
    //      iter.map(e => "PID = " + pid + ", value = " + e)
    //    }).foreach(println)

    val result = x.cartesian(y)



    println("Elements in result")
    result.mapPartitionsWithIndex((pid, iter) => {
      iter.map(e => "PID = " + pid + ", value = " + e)
    }).foreach(println)


    print(result.toDebugString)
    //Thread.sleep(100000)
  }
}
