import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Event_Sponsorship_SETB_28_SQL {
  def main(arg: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Event_Sponsorship_SETB_28")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val EventSponsorshipDF = Seq(
      (1,"Alpha Corp",1200,"2024-02-05"),
      (2,"Beta LLC",7000,"2024-02-10"),
      (3,"Gamma Inc",3000,"2024-02-15"),
      (4,"Delta Ltd",9000,"2024-02-20"),
      (5,"Epsilon Co",15000,"2024-02-25"),
      (6,"Zeta AG",4000,"2023-02-28")

    ).toDF("sponsor_id","sponsor_name","sponsorship_amount","sponsorship_date")

    EventSponsorshipDF.createOrReplaceTempView("EventSponsorship")

    val AmountCategoryDF = spark.sql(
      """SELECT
         sponsor_id,sponsor_name,sponsorship_amount,sponsorship_date,
         CASE
         WHEN sponsorship_amount > 10000 THEN "High"
         WHEN sponsorship_amount >=5000 AND sponsorship_amount <=10000 THEN "Medium"
         ELSE "Fail"
         END AS amount_category
         FROM EventSponsorship
        """)

    AmountCategoryDF.show()

    AmountCategoryDF.createOrReplaceTempView("AmountCategory")

    // Filter sponsorships where sponsorship_date is in 'February 2024'.

    val filterationDF = spark.sql(
      """
         SELECT *
         FROM EventSponsorship
         WHERE YEAR(TO_DATE(sponsorship_date,"yyyy-MM-dd")) = 2024
         AND MONTH(TO_DATE(sponsorship_date,"yyyy-MM-dd"))= 2

        """)

    filterationDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //sponsorship_amount for each amount_category

    val aggregationDF = spark.sql(
      """
       SELECT
       SUM(sponsorship_amount) AS Total_Amount,
       AVG(sponsorship_amount) AS AVG_Amount,
       MAX(sponsorship_amount) AS Maximum_Amount,
       MIN(sponsorship_amount) AS Minimum_Amount
       FROM AmountCategory
       GROUP BY amount_category
       """)

    aggregationDF.show()



  }
}
