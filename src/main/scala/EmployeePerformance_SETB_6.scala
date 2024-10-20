import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, month, to_date, when}

object EmployeePerformance_SETB_6 {
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

    val PerformanceScoreDF = EmployeePerfDF.withColumn("performance_category",
      when($"performance_score" >=9,"Excellent")
    .when($"performance_score" >=7 && $"performance_score"<9,"Good")
        .otherwise("Needs_Improvement")
    )
    PerformanceScoreDF.show()

    // Filter reviews where review_text contains 'excellent'.

    val ReviewExcellent = PerformanceScoreDF.filter($"review_text".contains("Excellent"))

    ReviewExcellent.show()

    // Calculate the average performance_score per month.

    val reviewMonth = PerformanceScoreDF.withColumn("convertedDate",to_date($"review_date","yyyy-MM-dd"))
      .withColumn("Month",month($"convertedDate"))
reviewMonth.show()

    val aggregationDF = reviewMonth.groupBy($"Month")
      .agg(avg($"performance_score").alias("Average_score"))

    aggregationDF.show()

  }
}