import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Product_Inventory_SETB_4_SQL {

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

    val ProductInventoryDF = Seq(
      (1, "Pro Widget", 30, "2024-01-10"),
      (2, "Pro Device", 120, "2024-01-15"),
      (3, "Standard", 200, "2024-01-20"),
      (4, "Pro Gadget", 40, "2024-02-01"),
      (5, "Standard", 60, "2024-02-10"),
      (6, "Pro Device", 90, "2024-03-01")
    ).toDF("product_id", "product_name", "stock", "last_restocked")

    ProductInventoryDF.createOrReplaceTempView("ProductInventory")

    val StockStatus = spark.sql(
      """SELECT
         product_id,product_name,stock,last_restocked,
         CASE
         WHEN stock < 50 THEN "Low"
         WHEN stock >= 50 AND stock <= 150 THEN "Medium"
         ELSE "High"
         END AS stock_status
         FROM ProductInventory


       """)

    StockStatus.show()
    StockStatus.createOrReplaceTempView("StockStatus")

    // Filter products where product_name contains 'Pro'.

    val ProductStartWithPRO = spark.sql(
      """
         SELECT *
         FROM StockStatus
         WHERE product_name LIKE '%Pro%'


        """)
    ProductStartWithPRO.show()

    // Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock for each
    //stock status

    val aggregationDF = spark.sql(
      """
         SELECT
         SUM(stock) AS Total_Stock,
         AVG(stock) AS Average_Stock,
         MAX(stock) AS Maximum_Stock,
         MIN(stock) AS Minimum_Stock
         FROM StockStatus
         GROUP BY stock_status


        """)

    aggregationDF.show()


  }
}
