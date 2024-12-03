import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, max, min, sum, when}

object MovieShowTime_SETB_12 {

  def main(args:Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","MovieShowTime_SETB_12")
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

   val MovieShowTime = ShowTimeDF.withColumn("availability",
     when($"seats_available"<=10,"Full")
       .when($"seats_available">=11 && $"seats_available"<=50,"Limited")
       .otherwise("Plenty")
   )

    MovieShowTime.show()

    // Filter showtimes where movie_title contains 'Action'.

    val MovieContainsActiondf = ShowTimeDF.filter($"movie_title".contains("Action"))

    MovieContainsActiondf.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //seats_available for each availability.

    val aggregationdf = MovieShowTime.groupBy("availability")
      .agg(sum($"seats_available").as("Total_Seats"),
        avg($"seats_available").as("Average_Seats"),
        max($"seats_available").as("Maximum_Seats"),
        min($"seats_available").as("Minimum_Seats")
      )
    aggregationdf.show()




  }

}
