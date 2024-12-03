import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ProductAnalysis_SETB_7_SQL {

  def main(args:Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","ProductAnalysis_SETB_7_SQL")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val ProductAnalysis = Seq(

      (1,"Smartphone",4,"2024-01-15"),
      (2,"Speaker",3,"2024-01-20"),
      (3,"Smartwatch",5,"2024-02-15"),
      (4,"Screen",2,"2024-02-20"),
      (5,"Speakers",4,"2024-03-05"),
      (6,"Soundbar",3,"2024-03-12")
    ).toDF("review_id","product_name","rating","review_date")

    ProductAnalysis.createOrReplaceTempView("ProductAnalysis")

    val ProductAnalysisDf = spark.sql(
      """SELECT
         review_id,product_name,rating,review_date,
         CASE
         WHEN rating >=4 THEN "High"
         WHEN rating >=3 AND rating <3 THEN "Medium"
         ELSE "Low"
         END AS rating_category
         FROM ProductAnalysis

        """)
    ProductAnalysisDf.show()
    ProductAnalysisDf.createOrReplaceTempView("ProductAnalysis")

    //Filter reviews where product_name starts with 'S'.
    val ProductStartsWithS = spark.sql(
      """SELECT *
         FROM ProductAnalysis
         WHERE product_name like 'S%'
        """)
    ProductStartsWithS.show()

    // Calculate the total count of reviews and average rating for each rating_category.

    val aggregationDF = spark.sql(
      """
        SELECT rating_category,
        COUNT(*) AS Total_reviews,
        AVG(rating) AS Average_Rating
        FROM ProductAnalysis
        GROUP BY rating_category
        """)

    aggregationDF.show()



  }

}
