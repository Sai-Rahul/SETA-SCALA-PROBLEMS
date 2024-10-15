import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Demo {

  def main(args:Array[String]) : Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Ass1_5")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val events = List(
      (1, "2024-07-27"),
      (2, "2024-12-25"),
      (3, "2025-01-01")
    ).toDF("event_id", "date")

    //How would you add a new column is_holiday which is true if the date is "2024-12-25" or
    //"2025-01-01", and false otherwise?

    val df = events.withColumn("is_holiday",
      when(col("date") === "2024-12-25","true")
        .when(col("date")==="2025-01-01", "true")
        .otherwise("false")

    )

    df.show()
  }


}
