import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object CustomerPurchase_SETB_9_SQL {

  def main(args:Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","CustomerPurchase_SETB_9_SQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val customerPurchase = Seq(
      (1,1,2500,"2024-01-05"),
      (2,2,1500,"2024-01-15"),
      (3,3,500,"2024-02-20"),
      (4,4,2200,"2024-03-01"),
      (5,5,900,"2024-01-25"),
      (6,6,3000,"2024-03-12")

    ).toDF("purchase_id","customer_id","purchase_amount","purchase_date")

    customerPurchase.createOrReplaceTempView("customerPurchase")

    val PurchaseHistorydf = spark.sql(
      """SELECT
         purchase_id,customer_id,purchase_amount,purchase_date,
         CASE
         WHEN purchase_amount >2000 THEN "Large"
         WHEN purchase_amount >=1000 AND purchase_amount <2000 THEN "Medium"
         ELSE "Small"
         END AS purchase_category
         FROM customerPurchase
       """)

    PurchaseHistorydf.show()
    PurchaseHistorydf.createOrReplaceTempView("PurchaseHistory")

    // Filter purchases that occurred in 'January 2024'.

    val PurchaseInJanuary = spark.sql(
      """SELECT *
         FROM PurchaseHistory
         WHERE to_date(purchase_date,"yyyy-MM-dd") BETWEEN '2024-01-01' AND '2024-01-31'
        """)

    PurchaseInJanuary.show()
    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //purchase_amount for each purchase_category.

    val aggregationDF = spark.sql(
      """
       SELECT purchase_category,
       SUM(purchase_amount) AS Total_Amount,
       AVG(purchase_amount) AS Average_Amount,
       MAX(purchase_amount) AS Maximum_Amount,
       MIN(purchase_amount) AS Minimum_Amount
       FROM PurchaseHistory
       GROUP BY purchase_category

       """)
    aggregationDF.show()



  }

}
