import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, sum, when,min}

object MovieRatingDuration_4 {
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
      (1,"The Matrix",9,136),
      (2,"Inception",8,148),
      (3,"The Godfather",9,175),
      (4,"Toy Story",7,81),
      (5,"The Shawshank Redemption",10,142),
      (6,"The Silence of the Lambs",8,118)
    ).toDF("movie_id","movie_name","rating","duration_minutes")

    val rating = moviedf.withColumn("rating_category",
      when($"rating">=8,"Excellent")
        .when($"rating">=6,"Good")
        .otherwise("Average")
    )

    val duration = moviedf.withColumn("duration_category",
      when($"duration_minutes">150,"Long")
        .when($"duration_minutes">=90,"Medium")
        .otherwise("Short")
    )

    rating.show()
    rating.createOrReplaceTempView("rating")
    duration.show()

    // Filter movies whose movie_name starts with 'T'.

    val MovieStartsWithT = moviedf.filter($"movie_name".startsWith("T"))
    MovieStartsWithT.show()

    // Filter movies whose movie_name ends with 'e'.
    val MovieEndsWithE = moviedf.filter($"movie_name".endsWith("E"))
    MovieEndsWithE.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //duration_minutes for each rating_category

    val aggregation = rating.groupBy("rating_category")
      .agg(sum($"duration_minutes").alias("Total_duartion"),
        avg($"duration_minutes").alias("Average_duration"),
        min($"duration_minutes").alias("Minimum_duration"),
        max($"duration_minutes").alias("Max_duration")
      )
    aggregation.show()


  }
}
