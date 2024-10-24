import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{count, explode}

object WordCount_GroupBy_4 {

  def main(arg: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "WordCount_GroupBy_4")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

      import spark.implicits._

      val textData = Seq(
        "Hello, how are you?",
        "I am fine, thank you!",
        "How about you?"
      ).toDF("Text")
      textData.show()

    // Split the text into words

    val words = textData.select(explode(functions.split($"Text", "\\s+")).alias("Word"))

    words.show()

    //Finding the count of occurrences for each word in a text document.

    val CountOcuurence = words.groupBy("Word")
      .agg(count($"Word").alias("word_Count"))

    CountOcuurence.show()


  }
}

