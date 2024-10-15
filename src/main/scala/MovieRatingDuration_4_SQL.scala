import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovieRatingDuration_4_SQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "MovieRatingDuration_4")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data
    val moviedf = Seq(
      (1, "The Matrix", 9, 136),
      (2, "Inception", 8, 148),
      (3, "The Godfather", 9, 175),
      (4, "Toy Story", 7, 81),
      (5, "The Shawshank Redemption", 10, 142),
      (6, "The Silence of the Lambs", 8, 118)
    ).toDF("movie_id", "movie_name", "rating", "duration_minutes")

    moviedf.createOrReplaceTempView("moviedf")

    val rating = spark.sql(
      """
         SELECT *,
         CASE
         WHEN rating > 8 THEN "Excellent"
         WHEN rating >= 6 THEN "Good"
         ELSE "Average"
         END AS rating_category
         FROM moviedf
       """)

    val duration = spark.sql(
      """
         SELECT *,
         CASE
         WHEN duration_minutes > 150 THEN "Long"
         WHEN duration_minutes >= 90 THEN "Medium"
         ELSE "Short"
         END AS duration_category
         FROM moviedf
        """)
    rating.show()
    rating.createOrReplaceTempView("rating")
    duration.show()

    //    // Filter movies whose movie_name starts with 'T'.

    val MovieStartsWithT = spark.sql(
      """
         SELECT *
         FROM moviedf
         WHERE movie_name LIKE 'T%'
         """)

    MovieStartsWithT.show()

    val MovieEndsWithE = spark.sql(
      """
         SELECT *
         FROM moviedf
         WHERE movie_name LIKE '%E'
         """)

    MovieEndsWithE.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //duration_minutes for each rating_category
    val aggregation = spark.sql(
      """
         SELECT rating_category,
         SUM(duration_minutes) AS Total_duartion,
         AVG(duration_minutes) AS Average_duration,
         MIN(duration_minutes) AS Minimum_duration,
         MAX(duration_minutes) AS Max_duration
         FROM rating
         GROUP BY rating_category


        """)
    aggregation.show()


  }
}
