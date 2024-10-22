import org.apache.spark.SparkConf
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum, when}

object Book_Store_Inventory_SETB_11 {

  def main(arg: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
     val sparkconf = new SparkConf()
     sparkconf.set("spark.app.name","Book_Store_Inventory_SETB_11")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val bookstoreDF = Seq(
      (1,"The Great Gatsby",150,"2024-01-10"),
      (2,"The Catcher in the Rye",80,"2024-01-15"),
      (3,"Moby Dick",200,"2024-01-20"),
      (4,"To Kill a Mockingbird",30,"2024-02-01"),
      (5,"The Odyssey",60,"2024-02-10"),
      (6,"War and Peace",20,"2024-03-01")
    )toDF("book_id","book_title","stock_quantity","last_updated")

    bookstoreDF.show()


    val stockLevelDF = bookstoreDF.withColumn("stock_Level",
      when($"stock_quantity" > 100, "High")
        .when($"stock_quantity" >=50 && $"stock_quantity"<=100,"Medium")
        .otherwise("Low"))
    stockLevelDF.show()

    // Filter books where book_title starts with 'The'.

    val BookTitleWithThe = bookstoreDF.filter($"book_title".startsWith("The"))
    BookTitleWithThe.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock_quantity
    //for each stock_level

    val aggregationDF = stockLevelDF.groupBy("stock_Level")
      .agg(sum($"stock_quantity").alias("Total_Stock"),
        avg($"stock_quantity").alias("Average_Stock"),
        max($"stock_quantity").alias("Maximum_Stock"),
        min($"stock_quantity").alias("MInimum_Stock")
      )
    aggregationDF.show()

  }

}
