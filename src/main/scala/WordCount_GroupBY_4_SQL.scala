import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object WordCount_GroupBY_4_SQL {
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

    textData.createOrReplaceTempView("textData")

    val WordOccurence = spark.sql(
      """
         SELECT
         explode(split(Text , '\\s+')) as word
         FROM textData

        """)
    WordOccurence.show()
    WordOccurence.createOrReplaceTempView("WordOccurence")
    //Finding the count of occurrences for each word in a text document.

    val WordCount = spark.sql(
      """SELECT word,
         COUNT(word) As Total_Count
         FROM WordOccurence
         GROUP BY word

       """)
    WordCount.show()


  }
}
