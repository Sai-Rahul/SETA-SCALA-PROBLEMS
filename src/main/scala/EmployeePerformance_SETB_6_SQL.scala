import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EmployeePerformance_SETB_6_SQL {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "EmployeePerformance_SETB_6")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val EmployeePerfDF = Seq(
      (1, "2024-01-10", 8, "Good performance"),
      (2, "2024-01-15", 9, "Excellent work"),
      (3, "2024-02-20", 6, "Needs improvement."),
      (4, "2024-02-25", 7, "Good Effort"),
      (5, "2024-03-05", 10, "Outstanding"),
      (6, "2024-03-12", 5, "Needs Improvement")
    ).toDF("employee_id", "review_date", "performance_score", "review_text")

    EmployeePerfDF.createOrReplaceTempView("EmployeePerformance")

    val PerformanceScoreDF = spark.sql(
      """SELECT
         employee_id,review_date,performance_score,review_text,
         CASE
         WHEN performance_score >=9 THEN "EXCELLENT"
         WHEN performance_score >=7 AND performance_score <9 THEN "GOOD"
         ELSE "Needs_Improvement"
         END AS performance_category
         FROM EmployeePerformance

        """)
    PerformanceScoreDF.show()

    PerformanceScoreDF.createOrReplaceTempView("PerformanceScore")

    // Filter reviews where review_text contains 'excellent'.

    val reviewTextExcellent = spark.sql(
      """
         SELECT *
         FROM PerformanceScore
         WHERE review_text LIKE '%Excellent%'
        """)
    reviewTextExcellent.show()

    // Calculate the average performance_score per month.

    val aggregationDF = spark.sql(
      """
         SELECT
         MONTH(TO_DATE(review_date,'yyyy-MM-dd'))AS Month,
         AVG(performance_score) AS Average_Score
         FROM EmployeePerformance
         GROUP BY Month
        """)
    aggregationDF.show()


  }
}
