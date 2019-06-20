import org.apache.spark.{SparkConf, SparkContext}

// please don't change the object name
object Task4 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 4")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    // modify this code
    val movieRatings = textFile
      .map(line => {
        val tokens = line.split(",", -1)
        (tokens(0), tokens.drop(1))
      })
      .collectAsMap()
    val movieRatingsBroadcast = sc.broadcast(movieRatings)

    val output = textFile
      .flatMap(line => {
        val tokens = line.split(",", -1)
        val movie1 = tokens(0)
        val ratings1 = tokens.drop(1)
        movieRatingsBroadcast.value.flatMap(entry => {
          val movie2 = entry._1
          val ratings2 = entry._2
          if (movie1 >= movie2) // lexicographic order
            List()
          else {
            val similarity = ratings1.zip(ratings2).count(x => x._1 == x._2 && x._1 != "")
            List(movie1 + "," + movie2 + "," + similarity)
          }
        })
      })

    output.saveAsTextFile(args(1))
  }
}
