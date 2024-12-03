import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SupplierTransactions_SETB_31_SQL {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "SupplierTransactions_SETB_31")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val SupplierTransactionsDF = Seq(

      (1, "Alpha Ltd", 16000, "2024-06-01"),
      (2, "Beta Inc", 8000, "2024-06-05"),
      (3, "Gamma LLC", 4000, "2024-06-10"),
      (4, "Delta Co", 12000, "2024-06-15"),
      (5, "Epsilon Ltd", 18000, "2024-06-20"),
      (6, "Zeta Corp", 3000, "2024-06-25")
    ).toDF("transaction_id", "supplier_name", "transaction_amount", "transaction_date")

    SupplierTransactionsDF.show()
    SupplierTransactionsDF.createOrReplaceTempView("SupplierTransactionsDF")


    val SupplierTransactions = spark.sql(
      """
         SELECT transaction_id,supplier_name,transaction_amount,transaction_date,
         CASE
         WHEN transaction_amount > 15000 THEN 'High'
         WHEN transaction_amount>=5000 AND transaction_amount<=15000 THEN 'Medium'
         ELSE 'Low'
         END AS transaction_status
         FROM SupplierTransactionsDF
        """)
    SupplierTransactions.show()

    SupplierTransactions.createOrReplaceTempView("SupplierTransactions")

    // Filter transactions where transaction_date is in 'June 2024'.

    val TransactionInJune = spark.sql(
      """SELECT *
         FROM SupplierTransactions
         WHERE to_date(transaction_date,'yyyy-MM-dd') BETWEEN '2024-06-01' AND '2024-06-30'
        """)

    TransactionInJune.show()
    TransactionInJune.createOrReplaceTempView("TransactionInJune")

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //transaction_amount for each transaction_status.

    val aggregationDF = spark.sql(
      """SELECT transaction_status,
         SUM(transaction_amount) AS Total_Amount,
         AVG(transaction_amount) AS Avg_Amount,
         MAX(transaction_amount) AS Max_Amount,
         MIN(transaction_amount) AS Min_Amount
         FROM SupplierTransactions
         GROUP BY transaction_status
        """)
    aggregationDF.show()

  }
}
