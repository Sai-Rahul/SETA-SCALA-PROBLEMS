import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, month, sum, to_date, when, year}

object Online_Purchase_History_SETB_15 {

  def main(arg: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Book_Store_Inventory_SETB_11")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val BookStoreInventoryDF = Seq(
      (1,1,700,"2024-02-05"),
      (2,2,150,"2024-02-10"),
      (3,3,400,"2024-02-15"),
      (4,4,600,"2024-02-20"),
      (5,5,250,"2024-02-25"),
      (6,6,1000,"2024-02-28")
    ).toDF("purchase_id","customer_id","purchase_amount","purchase_date")

    BookStoreInventoryDF.show()


    val purchaseStatus = BookStoreInventoryDF.withColumn("purchase_status",
      when($"purchase_amount">500,"High")
        .when($"purchase_amount" >=200 && $"purchase_amount" <=500, "Medium")
        .otherwise("Small")

    )

    purchaseStatus.show()

    // Filter purchases that occurred in the 'February 2024'.

    val ConverttoDateDF = BookStoreInventoryDF.withColumn("purchase_date_coverted",to_date($"purchase_date","yyyy-MM-dd"))
      .withColumn("month",month($"purchase_date_coverted"))
      .withColumn("year",year($"purchase_date_coverted"))

    ConverttoDateDF.show()

    val filteredDF = ConverttoDateDF.filter($"month"===2 && $"year"===2024)

    filteredDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //purchase_amount for each purchase_status

    val aggregationDF = purchaseStatus.groupBy("purchase_status")
      .agg(sum($"purchase_amount").alias("Total_Amount"),
        avg($"purchase_amount").alias("Average_Amount"),
        max($"purchase_amount").alias("Max_Amount"),
        min($"purchase_amount").alias("Min_Amount"))

    aggregationDF.show()


  }
}
