import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Customer_Purchase_History_SETB_9_SQL {
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
      (1, 1, 2500, "2024-01-05"),
      (2, 2, 1500, "2024-01-15"),
      (3, 3, 500, "2024-02-20"),
      (4, 4, 2200, "2024-03-01"),
      (5, 5, 900, "2024-01-25"),
      (6, 6, 3000, "2024-03-12")
    ).toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")

    CustomerPurchaseDF.createOrReplaceTempView("CustomerPurchase")

    val Purchasedf = spark.sql(
      """
         SELECT
         purchase_id,customer_id,purchase_amount,purchase_date,
         CASE
         WHEN purchase_amount >2000 THEN "Large"
         WHEN purchase_amount >=1000 AND purchase_amount <=2000 THEN "Medium"
         ELSE "Small"
         END AS purchase_category
         FROM
         CustomerPurchase

""")
    Purchasedf.show()

    Purchasedf.createOrReplaceTempView("Purchased")

    // Filter purchases that occurred in 'January 2024'.

    val PurchasedInJan = spark.sql(
      """
         SELECT *
         FROM Purchased
         WHERE YEAR(purchase_date) = 2024
         AND MONTH(purchase_date) = 1
        """
    )

    PurchasedInJan.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //purchase_amount for each purchase_category

    val aggregation = spark.sql(
      """
         SELECT
         SUM(purchase_amount) AS Total Purchase Amount,
         AVG(purchase_amount) AS Average purchase Amount,
         MAX(purchase_amount) AS Maximum Purchase Amount,
         MIN(purchase_amount) As Minimum Purchase Amount
         FROM Purchased
         GROUP BY purchase_category
        """)

    aggregation.show()




  }
}
