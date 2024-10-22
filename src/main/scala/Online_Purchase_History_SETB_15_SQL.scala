import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum}

object Online_Purchase_History_SETB_15_SQL {

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
      (1, 1, 700, "2024-02-05"),
      (2, 2, 150, "2024-02-10"),
      (3, 3, 400, "2024-02-15"),
      (4, 4, 600, "2024-02-20"),
      (5, 5, 250, "2024-02-25"),
      (6, 6, 1000, "2024-02-28")
    ).toDF("purchase_id", "customer_id", "purchase_amount", "purchase_date")

    BookStoreInventoryDF.createOrReplaceTempView("BookStoreInventory")

    val purchaseStatus = spark.sql(
      """
         SELECT
         purchase_id,customer_id,purchase_amount,purchase_date,
         CASE
         WHEN purchase_amount>500 THEN "High"
         WHEN purchase_amount >=200 AND purchase_amount <=500 THEN "Medium"
         ELSE "Low"
         END AS purchase_status
         FROM BookStoreInventory

        """)

    purchaseStatus.show()
    purchaseStatus.createOrReplaceTempView("purchaseStatus")

    // Filter purchases that occurred in the 'February 2024'.

    val filteredDF = spark.sql(
      """
         SELECT *
         FROM
         purchaseStatus
         WHERE MONTH(purchase_date) = 2 AND YEAR(purchase_date) = 2024


       """)

    filteredDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //purchase_amount for each purchase_status

    val aggregationDF = spark.sql(
      """
        SELECT
        SUM(purchase_amount) AS Total_Amount,
        AVG(purchase_amount) AS AVG_Amount,
        MAX(purchase_amount) AS Maximum_Amount,
        MIN(purchase_amount) AS Minimum_Amount
        FROM purchaseStatus
        GROUP BY purchase_status
        """)

    aggregationDF.show()



  }
}
