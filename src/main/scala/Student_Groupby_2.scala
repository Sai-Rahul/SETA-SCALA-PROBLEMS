import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, max}

object Student_Groupby_2 {

  def main(arg: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Student_Groupby_2")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val scoreData = Seq(
      ("Alice", "Math", 80),
      ("Bob", "Math", 90),
      ("Alice", "Science", 70),
      ("Bob", "Science", 85),
      ("Alice", "English", 75),
      ("Bob", "English", 95)
    ).toDF("Student", "Subject", "Score")

    scoreData.show()

    // Group by Subject and calculate the average score

    val averageScoreBySubject = scoreData.groupBy("Subject")
      .agg(avg($"Score").alias("average_Score"))

    averageScoreBySubject.show()

    // Group by Student and calculate the maximum score

    val maximumScore = scoreData.groupBy("Student")
      .agg(max($"Score").alias("Maximum_score"))

    maximumScore.show()


  }
}
