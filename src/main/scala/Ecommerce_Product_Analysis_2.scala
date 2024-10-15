import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, col, sum, when, max, min}

object Ecommerce_Product_Analysis_2 {

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

    val Price_df = EcommerceData.withColumn("price_category",
      when(col("price") > 500, "Expensive")
        .when(col("price") < 500, "Moderate")
        .otherwise("Cheap")
    )

    Price_df.show()

    // Filter products whose product_name starts with 'S'.

    val ProductNameWithS = Price_df
      .filter($"product_name".startsWith("S"))
    ProductNameWithS.show()

    // Filter products whose product_name ends with 's'.

    val productNameEndsWithS = Price_df.filter($"product_name".endsWith("s"))
    productNameEndsWithS.show()

    //Calculate the total price (sum), average price, maximum price,
    // and minimum price for each category.

    val Aggregations = Price_df
      .groupBy("category")
      .agg(sum($"price").alias("Total_Price"),
      avg($"price").alias("Average_Price"),
        max($"price").alias("Maximum_price"),
        min($"price").alias("Minimum_price")


      )
    Aggregations.show()

  }
}
