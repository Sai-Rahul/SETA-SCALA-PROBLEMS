import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum, to_date, when, year}

object CustomerTransaction_SETB_5 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Product_Inventory_SETB_4")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val customerTransactionDF = Seq(
      (1,1,1200,"2024-01-15"),
      (2,2,600,"2024-01-20"),
      (3,3,300,"2024-02-15"),
      (4,4,1500,"2024-02-20"),
      (5,5,200,"2024-03-05"),
      (6,6,900,"2024-03-12")
    ).toDF("transaction_id","customer_id","transaction_amount","transaction_date")
    //customerTransactionDF.show()

    val Transaction_categoryDF = customerTransactionDF.withColumn("transaction_category",
      when($"transaction_amount">1000, "High")
        .when($"transaction_amount">=500 && $"transaction_amount"<=1000,"Medium")
        .otherwise("Low")
    )
    Transaction_categoryDF.show()

    // Filter transactions where transaction_date is in 2024.

    val Transaction_dateDF = Transaction_categoryDF.withColumn("trans_date",to_date($"transaction_date","yyyy-MM-dd"))
      .withColumn("year",year($"trans_date"))
    Transaction_dateDF.show()

    val TransactionWith2024 = Transaction_dateDF.filter($"year"===2024)
    TransactionWith2024.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //transaction_amount for each category.

    val aggregationsDF = Transaction_categoryDF.groupBy($"transaction_category")
      .agg(sum($"transaction_amount").alias("Total_Amount"),
        avg($"transaction_amount").alias("average_Amount"),
        max($"transaction_amount").alias("Maximum_Amount"),
        min($"transaction_amount").alias("Minimum_Amount")


      )

    aggregationsDF.show()




  }
}

