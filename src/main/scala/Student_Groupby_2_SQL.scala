import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Student_Groupby_2_SQL {
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

    scoreData.createOrReplaceTempView("scoreData")

    //Group by Subject and calculate the average score

    val averageScoreBySubject = spark.sql(
      """
         SELECT Subject,
         AVG(Score) AS Average_Score
         FROM scoreData
         GROUP BY Subject

        """)

    averageScoreBySubject.show()

    //// Group by Student and calculate the maximum score

    val MaxScore = spark.sql(
      """
         SELECT Student,
         MAX(Score) AS Max_Score
         FROM scoreData
         GROUP BY Student
       """)

    MaxScore.show()


  }
}
