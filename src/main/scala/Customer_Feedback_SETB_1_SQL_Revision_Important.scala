import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Customer_Feedback_SETB_1_SQL_Revision_Important {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Customer_Feedback_SETB_1")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data

    val customer = Seq(
      (1, "2024-01-10", 4, "Great service!"),
      (2, "2024-01-15", 5, "Excellent!"),
      (3, "2024-02-20", 2, "Poor experience"),
      (4, "2024-02-25", 3, "Good Value"),
      (5, "2024-03-05", 4, "Great quality"),
      (6, "2024-03-12", 1, "Bad service")).toDF("customer_id", "feedback_date", "rating", "feedback_text")

    customer.createOrReplaceTempView("customer")

    val rating = spark.sql(
      """
         SELECT customer_id,
           feedback_date,
           rating,
           feedback_text,
         CASE
         WHEN rating >= 5 THEN "Excellent"
         WHEN rating >=3 AND rating <5 THEN "Good"
         ELSE "Poor"
         END AS rating_category
         FROM customer
        """)
    rating.show()
    rating.createOrReplaceTempView("rating")
    // Filter feedback with feedback_text that starts with 'Great'.
val feedback = spark.sql(
  """
     SELECT *
     FROM rating
     WHERE feedback_text LIKE '%Great%'
    """)
feedback.show()

    // Calculate the average rating per month.

    val AvgRating = spark.sql(
      """SELECT
         month(feedback_date) AS month,
         avg(rating) AS Average_Rating
         FROM rating
         GROUP BY(feedback_date)
         ORDER BY(feedback_date)




        """)
    AvgRating.show()
  }
}
