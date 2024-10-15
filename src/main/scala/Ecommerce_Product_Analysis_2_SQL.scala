import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Ecommerce_Product_Analysis_2_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Ecommerce_Product_Analysis_2")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data

    val EcommerceData = Seq(
      (1, "SmartPhone", 700, "Electronics"),
      (2, "TV", 1200, "Electonics"),
      (3, "Shoes", 150, "Apparel"),
      (4, "Socks", 25, "Apparel"),
      (5, "Laptop", 800, "Electronics"),
      (6, "Jacket", 200, "Apparel")
    ).toDF("product_id", "product_name", "price", "category")

    EcommerceData.createOrReplaceTempView("EcommerceData")

    val Price_df = spark.sql(
      """
         SELECT *,
         CASE
         WHEN price >500 THEN "Expensive"
         WHEN price >400 THEN "Moderate"
         ELSE "Cheap"
         END As price_category
         FROM EcommerceData
        """)
    Price_df.show()

    Price_df.createOrReplaceTempView("Price_df")
    // Filter products whose product_name starts with 'S'.
    val ProductNameWithS = spark.sql(
      """
         SELECT *
         FROM Price_df
         WHERE product_name LIKE "S%"

        """)
    ProductNameWithS.show()

    //Price_df.createOrReplaceTempView("Price_df")

    val ProductNameEndsWithS = spark.sql(
      """
         SELECT *
         FROM Price_df
         WHERE product_name LIKE '%s'

        """)
    ProductNameEndsWithS.show()

    //Calculate the total price (sum), average price, maximum price,
    // and minimum price for each category.

    val finalDF = spark.sql(
      """
         SELECT category,
         SUM(price) AS Total_Price,
         AVG(price) AS Average_Price,
         MAX(price) AS Maximum_Price,
         MIN(price) AS Minimum_Price
         FROM EcommerceData
         GROUP BY category


        """


    )

    finalDF.show()

  }
}
