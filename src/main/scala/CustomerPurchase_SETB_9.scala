import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum, to_date, when}

object CustomerPurchase_SETB_9 {

  def main(args:Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","CustomerPurchase_SETB_9")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val customerPurchase = Seq(
      (1,1,2500,"2024-01-05"),
      (2,2,1500,"2024-01-15"),
      (3,3,500,"2024-02-20"),
      (4,4,2200,"2024-03-01"),
      (5,5,900,"2024-01-25"),
      (6,6,3000,"2024-03-12")

    ).toDF("purchase_id","customer_id","purchase_amount","purchase_date")

    customerPurchase.show()


    val PurchaseHistorydf = customerPurchase.withColumn("purchase_category",
      when($"purchase_amount">2000,"Large")
        .when($"purchase_amount">=1000 && $"purchase_amount"<2000,"Medium")
        .otherwise("Small")
    )
    PurchaseHistorydf.show()
    // Filter purchases that occurred in 'January 2024'.
    val PurchaseInJanuary = customerPurchase.filter(to_date($"purchase_date","yyyy-MM-dd")
      .between("2024-01-01","2024-01-31")
    )

    PurchaseInJanuary.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //purchase_amount for each purchase_category.

    val aggregationDF = PurchaseHistorydf.groupBy("purchase_category")
      .agg(sum($"purchase_amount").as("Total_Purchase_Amount"),
        avg($"purchase_amount").as("Average_Amount"),
        max($"purchase_amount").as("Maximum_Amount"),
        min($"purchase_amount").as("Minimum_Amount")



      )
    aggregationDF.show()



  }

}
