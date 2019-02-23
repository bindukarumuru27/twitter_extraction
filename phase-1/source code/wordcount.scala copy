import org.apache.spark._
import org.apache.spark.SparkContext._
object WordCount {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("WordCount")

      // Create a Scala Spark Context.
      val sc = new SparkContext(conf)

      // hashtagsFile RDD Creation
      val hashtagsFile = sc.textFile("/users/bindukarumuru/hadoop/input/hashtags.txt")

      println("Loaded File")

      // hashtagsWords are words which are formed by flattening the each line from hashtagsFile
      val hashtagsWords = hashtagsFile.flatMap(line => line.split(" "))

      // sums up occurrences by each word
      val hashtagsCounts = hashtagsWords.map(word => (word, 1)).reduceByKey(_ + _)

      // sorts words by count
      println("Saving to file")
      val hashtagsSortedCount = hashtagsCounts.sortBy(-_._2)

      //saves output to the file
      hashtagsSortedCount.saveAsTextFile("/users/bindukarumuru/hadoop/hashtags_output_scala")

      println("Saved to file")

      val urlFile = sc.textFile("/users/bindukarumuru/hadoop/input/urls.txt")
      val urlWords = urlFile.flatMap(line => line.split(" "))
      val urlCounts = urlWords.map(word => (word, 1)).reduceByKey(_ + _)
      val urlSortedCount = urlCounts.sortBy(-_._2)
      urlSortedCount.saveAsTextFile("/users/bindukarumuru/hadoop/urls_output")

  }
}

