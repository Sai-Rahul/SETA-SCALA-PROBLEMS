import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, month, to_date, when}

object Customer_Feedback_SETB_1_Revision_Important {
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
      (1,"2024-01-10",4,"Great service!"),
      (2,"2024-01-15",5,"Excellent!"),
      (3,"2024-02-20",2,"Poor experience"),
      (4,"2024-02-25",3,"Good Value"),
      (5,"2024-03-05",4,"Great quality"),
      (6,"2024-03-12",1,"Bad service")).toDF("customer_id","feedback_date","rating","feedback_text")
    customer.show()

    val rating = customer.withColumn("rating_category",
      when($"rating" >=5,"Excellent")
        .when($"rating" >= 3 && $"rating" < 5,"Good")
        .otherwise("Poor")
    )
    rating.show()

    // Filter feedback with feedback_text that starts with 'Great'.

    val feedback = rating.filter($"feedback_text".startsWith("Great"))
    feedback.show()
    // Calculate the average rating per month.
    val FeedbackWithMonthDF = rating.withColumn("feedback_date", to_date($"feedback_date","yyyy-MM-dd"))
      .withColumn("month",month($"feedback_date"))

    val average = FeedbackWithMonthDF.groupBy("month")
      .agg(avg($"rating").alias("averagepermonth"))

    average.show()


  }
}
