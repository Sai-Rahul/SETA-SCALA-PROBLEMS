import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Book_Store_Inventory_SETB_11_SQL {

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

    val bookstoreDF = Seq(
      (1, "The Great Gatsby", 150, "2024-01-10"),
      (2, "The Catcher in the Rye", 80, "2024-01-15"),
      (3, "Moby Dick", 200, "2024-01-20"),
      (4, "To Kill a Mockingbird", 30, "2024-02-01"),
      (5, "The Odyssey", 60, "2024-02-10"),
      (6, "War and Peace", 20, "2024-03-01")
    ) toDF("book_id", "book_title", "stock_quantity", "last_updated")

    bookstoreDF.createOrReplaceTempView("bookstore")

    val stockLevelDF = spark.sql(
      """SELECT
         book_id,book_title,stock_quantity,last_updated,
         CASE
         WHEN stock_quantity > 100 THEN "High"
         WHEN stock_quantity >=50 AND stock_quantity <=100 THEN "Medium"
         ELSE "Low"
         END AS stock_Level
         FROM bookstore
        """)
    stockLevelDF.show()

    stockLevelDF.createOrReplaceTempView("stockLevel")

    // Filter books where book_title starts with 'The'.

    val BookTitleWithThe = spark.sql(
      """
        SELECT *
        FROM bookstore
        WHERE book_title LIKE '%The%'

        """)
    BookTitleWithThe.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock_quantity
    //for each stock_level

    val aggregationDF = spark.sql(
      """
         SELECT
         SUM(stock_quantity) AS Total_Stock,
         AVG(stock_quantity) AS Avg_Stock,
         MAX(stock_quantity) AS Max_Stock,
         MIN(stock_quantity) AS Min_Stock
         FROM stockLevel
         GROUP BY stock_Level
        """)

    aggregationDF.show()




  }
}
