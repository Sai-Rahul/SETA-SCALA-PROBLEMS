import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Product_Sales_SETB_2_SQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Product_Sales_SETB_2")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.memory.executor", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data

    val Sales = Seq(
      (1, "Widget", 700, "2024-01-15"),
      (2, "Gadget", 150, "2024-01-20"),
      (3, "Widget", 350, "2024-02-15"),
      (4, "Device", 600, "2024-02-20"),
      (5, "Widget", 100, "2024-03-05"),
      (6, "Gadget", 500, "2024-03-12")
    ).toDF("sale_id", "product_name", "sale_amount", "sale_date")
    Sales.createOrReplaceTempView("Sales")


    val SalesWithCategory = spark.sql(
      """SELECT
         sale_id,product_name,sale_amount,sale_date,
         CASE
         WHEN sale_amount >500 THEN "High"
         WHEN sale_amount >=200 AND sale_amount <500 THEN "Medium"
         ELSE "Low"
         END AS sale_category
         FROM Sales
        """)
    SalesWithCategory.show()

    // Filter sales where product_name ends with 't'.

    val ProductWithT = spark.sql(
      """
         SELECT *
       FROM Sales
       WHERE product_name LIKE 't%'
        """)

    ProductWithT.show()

//Aggregations

    val result = spark.sql(
      """SELECT
         date_format(to_date(sale_date,'yyyy-MM-dd'),'yyyy-MM') AS Month,
         SUM(sale_amount) AS Total,
         AVG(sale_amount) AS Average,
         MAX(sale_amount) AS MAX,
         MIN(sale_amount) AS MIN
         FROM Sales
         Group By Month


        """)

    result.show()

  }
}
