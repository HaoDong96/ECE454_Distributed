import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task2 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 2")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile
      .map(line => {
        val tokens = line.split(",")
        tokens.drop(1).count(_ != "")
      })
      .reduce(_ + _)

    sc.parallelize(Seq(output)).coalesce(1).saveAsTextFile(args(1))
  }
}
