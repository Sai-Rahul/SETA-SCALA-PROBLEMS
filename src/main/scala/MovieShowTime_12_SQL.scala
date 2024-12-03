import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object MovieShowTime_12_SQL {

  def main(args:Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","MovieShowTime_12_SQl")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._
    val ShowTimeDF = Seq(
      (1,"Action Hero","2024-01-10",8),
      (2,"Comedy Nights","2024-01-15",25),
      (3,"Action Packed","2024-01-20",55),
      (4,"Romance Special"," 2024-02-01",5),
      (5,"Action Force","2024-02-10",45),
      (6,"Drama Series","2024-03-01",70)
    ).toDF("show_id","movie_title","showtime","seats_available")

    ShowTimeDF.createOrReplaceTempView("ShowTime")

    val MovieShowTime = spark.sql(
      """SELECT
         show_id,movie_title,showtime,seats_available,
         CASE
         WHEN seats_available <=10 THEN "Full"
         WHEN seats_available >=11 AND seats_available <=50 THEN "Limited"
         ELSE "Plenty"
         END AS availability
         FROM ShowTime
        """)
    MovieShowTime.createOrReplaceTempView("MovieShowTime")

    // Filter showtimes where movie_title contains 'Action'.

    val MovieContainsActiondf = spark.sql(
      """SELECT *
         FROM ShowTime
         WHERE movie_title LIKE '%Action%'
        """)
    MovieContainsActiondf.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //seats_available for each availability.

    val aggregationDF = spark.sql(
      """SELECT availability,
         SUM(seats_available) AS Total_Seats,
         AVG(seats_available) AS Average_Seats,
         MAX(seats_available) AS Maximum_Seats,
         MIN(seats_available) AS Minimum_Seats
         FROM MovieShowTime
         GROUP BY availability
        """)

    aggregationDF.show()




  }

}
