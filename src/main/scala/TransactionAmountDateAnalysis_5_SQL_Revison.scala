import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object TransactionAmountDateAnalysis_5_SQL_Revison {def main(args: Array[String]): Unit = {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkconf = new SparkConf()
  sparkconf.set("spark.app.name", "TransactionAmountDateAnalysis_5_Revision_Revision")
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

  TransactionDF.createOrReplaceTempView("Transaction")

  val amount_categorydf = spark.sql(
    """
       SELECT *,
       CASE
       WHEN amount >1000 THEN "High"
       WHEN amount >=500 THEN "Medium"
       ELSE "Low"
       END AS amount_category
       FROM Transaction
      """)
  amount_categorydf.show()

  // Create a new column transaction_month that extracts the month from transaction_date.

  val TransactionDFWithMonth = spark.sql(
    """
       SELECT *,
       month(TO_DATE(transaction_date, 'yyyy-MM-dd')) AS Transaction_month
       FROM Transaction
       """)
  TransactionDFWithMonth.show()

  TransactionDFWithMonth.createOrReplaceTempView("TransactionDFWithMonth")
  //Filter transactions that occurred in the month of 'December'.

  // Query transactions where the month is December (12)
  val DecemberTransaction = spark.sql(
    """
    SELECT *
    FROM TransactionDFWithMonth
    WHERE transaction_month = 12
  """
  )

  // Show the result
  DecemberTransaction.show()
val Aggregation = spark.sql(
  """
     SELECT transaction_type,
     SUM(amount) AS Total_Amount,
     AVG(amount) As Average_Amount,
     MIN(amount) As Min_Amount,
     MAX(amount) As Max_Amount
     FROM TransactionDFWithMonth
     GROUP BY transaction_type
""")

  Aggregation.show()


}
}
