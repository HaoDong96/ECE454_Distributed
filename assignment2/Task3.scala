import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task3 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 3")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile
      .flatMap(line => {
        val rates = line.split(",").drop(1)
        (for {i <- rates.indices} yield (i + 1, if (rates(i) == "") 0 else 1)).toList
      })
      .reduceByKey(_ + _)
      .map(e => e._1 + "," + e._2)
    
    output.saveAsTextFile(args(1))
  }
}
