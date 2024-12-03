import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level,Logger}

object UtilityBill_SETB_21_SQL {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","UtilityBill_SETB_21_SQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val UtilityBillDF = Seq(
      (1,1,250,"2024-02-05"),
      (2,2,80,"2024-02-10"),
      (3,3,150,"2024-02-15"),
      (4,4,220,"2024-02-20"),
      (5,5,90,"2024-02-25"),
      (6,6,300,"2024-03-28")
    ).toDF("bill_id","customer_id","bill_amount","billing_date")

    UtilityBillDF.createOrReplaceTempView("UtilityBillDF")

    val UtilityBills = spark.sql(
      """SELECT
         bill_id,customer_id,bill_amount,billing_date,
         CASE
         WHEN bill_amount >200 THEN "High"
         WHEN bill_amount>=100 AND bill_amount<=200 THEN "Medium"
         ELSE "Low"
         END AS bill_status
         FROM UtilityBillDF
        """)
    UtilityBills.show()
    UtilityBills.createOrReplaceTempView("UtilityBills")

    // Filter bills where billing_date is in 'February 2024'.
    val DateConvDF = spark.sql(
      """
    SELECT *
    FROM UtilityBills
    WHERE to_date(billing_date, 'yyyy-MM-dd') BETWEEN '2024-02-01' AND '2024-02-29'
  """
    )
    DateConvDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) bill_amount
    //for each bill_status

    val aggregationDF = spark.sql(
      """
        SELECT bill_status,
        SUM(bill_amount) AS Total_Amount,
        AVG(bill_amount) AS Avg_Amount,
        MAX(bill_amount) AS Max_Amount,
        MIN(bill_amount) AS Min_Amount
        FROM UtilityBills
        GROUP BY bill_status

        """)

    aggregationDF.show()




  }

}
