import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, max, min, month, sum, to_date, when}
import org.apache.spark.sql.types.DateType

object TransactionAmountDateAnalysis_5_Revison {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "TransactionAmountDateAnalysis_5_Revision")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data

    val TransactionDF = Seq(
      (1,"2023-12-01",1200,"Credit"),
      (2,"2023-11-15",600,"Debit"),
      (3,"2023-12-20",300,"Credit"),
      (4,"2023-10-10",1500,"Debit"),
      (5,"2023-12-30",250,"Credit"),
      (6,"2023-09-25",700,"Debit")).toDF("transaction_id","transaction_date","amount","transaction_type")

    val transactions = TransactionDF.withColumn("amount_category",

      when($"amount">1000,"High")
        .when($"amount" >=500,"Medium")
        .otherwise("Low")

    )
    transactions.show()
    // Create a new column transaction_month that extracts the month from transaction_date.

    val transactionDFWithMonth = transactions
      .withColumn("transaction_date", to_date($"transaction_date","yyyy-MM-dd"))
      .withColumn("transaction_month",month($"transaction_date"))

    transactionDFWithMonth.show()

    //Filter transactions that occurred in the month of 'December'.

    val DecemberTransaction  = transactionDFWithMonth.filter($"transaction_month" ===12)

    DecemberTransaction.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) amount for
    //each transaction_type.

    val  aggregation = transactions.groupBy("transaction_type")
      .agg(sum($"amount").alias("Total_amount"),
      avg($"amount").alias("Average_amount"),
        max($"amount").alias("Max_amount"),
        min($"amount").alias("Min_amount")



      )

    aggregation.show()



  }
}
