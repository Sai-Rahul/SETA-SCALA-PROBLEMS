import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum, to_date, when}
import org.apache.log4j.{Level, Logger}

object SupplierTransactions_SETB_31 {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SupplierTransactions_SETB_31")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val SupplierTransactionsDF = Seq(

      (1,"Alpha Ltd",16000,"2024-06-01"),
      (2,"Beta Inc",8000,"2024-06-05"),
      (3,"Gamma LLC",4000,"2024-06-10"),
      (4,"Delta Co",12000,"2024-06-15"),
      (5,"Epsilon Ltd",18000,"2024-06-20"),
      (6,"Zeta Corp",3000,"2024-06-25")
    ).toDF("transaction_id","supplier_name","transaction_amount","transaction_date")

    SupplierTransactionsDF.show()

    val SupplierTransactions = SupplierTransactionsDF.withColumn("transaction_status",
      when($"transaction_amount">15000,"High")
        .when($"transaction_amount">=5000 && $"transaction_amount"<=15000,"Medium")
        .otherwise("Low")
    )
    SupplierTransactions.show()

    // Filter transactions where transaction_date is in 'June 2024'.

    val TransactionInJune = SupplierTransactions.filter(to_date($"transaction_date","yyyy-MM-dd").between("2024-06-01","2024-06-30"))
    TransactionInJune.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //transaction_amount for each transaction_status.

    val aggregationDF = SupplierTransactions.groupBy("transaction_status")
      .agg(sum($"transaction_amount").as("Total_Amount"),
      avg($"transaction_amount").as("Avg_Amount"),
        max($"transaction_amount").as("max_Amount"),
        min($"transaction_amount").as("Min_Amount"))

    aggregationDF.show()

  }

}
