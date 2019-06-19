import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val output = textFile
      .cartesian(textFile)
      .flatMap(line => {
        val tokens1 = line._1.split(",")
        val tokens2 = line._2.split(",")
        val movie1 = tokens1(0)
        val movie2 = tokens2(0)
        if (movie1 >= movie2) // lexicographic order
          List()
        else {
          val similarity = tokens1.zip(tokens2).count(x => x._1 == x._2 && x._1 != "")
          List(movie1 + "," + movie2 + "," + similarity)
        }
      })

    output.saveAsTextFile(args(1))
  }
}
