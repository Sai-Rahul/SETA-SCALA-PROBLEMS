import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, month, sum, to_date, when, year}

object Customer_Purchase_History_SETB_9 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Customer_Purchase_History_SETB_9")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val CustomerPurchaseDF = Seq(
      (1,1,2500,"2024-01-05"),
      (2,2,1500,"2024-01-15"),
      (3,3,500,"2024-02-20"),
      (4,4,2200,"2024-03-01"),
      (5,5,900,"2024-01-25"),
      (6,6,3000,"2024-03-12")
    ).toDF("purchase_id","customer_id","purchase_amount","purchase_date")

    CustomerPurchaseDF.show()

    val Purchasedf = CustomerPurchaseDF.withColumn("purchase_category",
      when($"purchase_amount">2000,"Large")
        .when($"purchase_amount" >=1000 && $"purchase_amount"<=2000,"Medium")
        .otherwise("Small")
    )
    Purchasedf.show()

    // Filter purchases that occurred in 'January 2024'.

    val PurchasedInJan = CustomerPurchaseDF.filter(month($"purchase_date") === 1 && year($"purchase_date")===2024)
    PurchasedInJan.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //purchase_amount for each purchase_category

    val aggregations = Purchasedf.groupBy($"purchase_category")
      .agg(sum($"purchase_amount").alias("Total Purchase Amount"),
      avg($"purchase_amount").alias("Average purchase Amount"),
      max($"purchase_amount").alias("Maximum_Purchase_Amount"),
        min($"purchase_amount").alias("Minimum_Purchase_Amount")
      )
    aggregations.show()



  }
}
