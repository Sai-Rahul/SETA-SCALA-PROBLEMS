import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SalespersonPerformance_SETB_43_SQL_Very_Important {

    def main(args: Array[String]): Unit = {
      Logger.getLogger("akka").setLevel(Level.OFF)
      Logger.getLogger("org").setLevel(Level.OFF)

      val sparkconf = new SparkConf()
      sparkconf.set("spark.app.name", "SalespersonPerformance")
      sparkconf.set("spark.master", "local[*]")
      sparkconf.set("spark.executor.memory", "2g")

      val spark = SparkSession.builder()
        .config(sparkconf)
        .getOrCreate()

      import spark.implicits._
      val SalesPerson = Seq(
        (1, 2500, "2024-12-01"),
        (2, 15000, "2024-12-05"),
        (3, 8000, "2024-12-10"),
        (4, 12000, "2024-12-15"),
        (5, 18000, "2024-12-20"),
        (6, 5000, "2024-12-25")
      ).toDF("salesperson_id", "sales_amount", "sales_date")

      SalesPerson.createOrReplaceTempView("SalesPerson")

      val SalesPersonPerformance = spark.sql(
        """SELECT
           salesperson_id,sales_amount,sales_date,
          CASE
          WHEN sales_amount >20000 THEN 'Top Performer'
          WHEN sales_amount>=10000 AND sales_amount <=20000 THEN 'Average Performer'
          ELSE 'Low Performer'
          END AS performance_category
          FROM SalesPerson


          """)
      SalesPersonPerformance.createOrReplaceTempView("SalesPersonPerformance")

      // Filter records where sales_date is in 'December 2024'.

      val SalesInDec = spark.sql(
        """
         SELECT *
         FROM SalesPersonPerformance
         WHERE YEAR(TO_DATE(sales_date)) =2024 AND MONTH(TO_DATE(sales_date)) =12

         """)
      SalesInDec.show()

      //Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount
      //for each performance_category.

      val aggregationDF = spark.sql(
        """SELECT performance_category,
           SUM(sales_amount) AS Total_Amount,
           AVG(sales_amount) AS Avg_Amount,
           MAX(sales_amount) AS Max_Amount,
           MIN(Sales_amount) AS Min_Amount
           FROM SalesPersonPerformance
           GROUP BY performance_category
          """)

      aggregationDF.show()




    }
  }
