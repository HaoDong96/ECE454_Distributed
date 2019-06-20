import org.apache.spark.{SparkContext, SparkConf}

// please don't change the object name
object Task1 {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Task 1")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile(args(0))

    val output = textFile
        .map(line => {
          val tokens = line.split(",",-1)
          val movieName = tokens(0)
          val rates = tokens.drop(1).map(num => {if (num == "") 0 else num.toInt})
          val max = rates.max
          val res = rates.indices.filter(r => rates(r) == max).map(_ + 1).mkString(",")
          (movieName, res)
        })
        .map(s => s._1 + "," + s._2)

    output.saveAsTextFile(args(1))
  }
}
