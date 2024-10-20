import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CustomerTransaction_SETB_5_SQL {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "CustomerTransaction_SETB_5_SQL")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val customerTransactionDF = Seq(
      (1, 1, 1200, "2024-01-15"),
      (2, 2, 600, "2024-01-20"),
      (3, 3, 300, "2024-02-15"),
      (4, 4, 1500, "2024-02-20"),
      (5, 5, 200, "2024-03-05"),
      (6, 6, 900, "2024-03-12")
    ).toDF("transaction_id", "customer_id", "transaction_amount", "transaction_date")
    //customerTransactionDF.show()

    customerTransactionDF.createOrReplaceTempView("customerTransaction")

    val Transaction_categoryDF = spark.sql(
      """SELECT
         transaction_id,customer_id,transaction_amount,transaction_date,
         CASE
         WHEN transaction_amount >1000 THEN "HIGH"
         WHEN transaction_amount >=500 AND transaction_amount <=1000 THEN "MEDIUM"
         ELSE "LOW"
         END AS trans_category
         FROM customerTransaction
        """)

    Transaction_categoryDF.show()

    Transaction_categoryDF.createOrReplaceTempView("Transaction_category")

    // Filter transactions where transaction_date is in 2024.

    val Transaction_dateDF = spark.sql(
      """
        SELECT *
       FROM Transaction_category
       WHERE YEAR(TO_DATE(transaction_date,'yyyy-MM-dd')) =2024

        """)
    Transaction_dateDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //transaction_amount for each category.

    val aggregationDF = spark.sql(
      """
         SELECT
         SUM(transaction_amount) AS TOTAL_AMOUNT,
         AVG(transaction_amount) AS Average_amount,
         MAX(transaction_amount) AS Maximum_amount,
         MIN(transaction_amount) AS Minimum_amount
         FROM Transaction_category
         GROUP BY trans_category
        """)

    aggregationDF.show()


  }
}

