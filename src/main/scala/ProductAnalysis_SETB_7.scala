import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, when}

object ProductAnalysis_SETB_7 {

  def main(args:Array[String]): Unit = {

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","ProductAnalysis_SETB_7")
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

    ProductAnalysis.show()

    val ProductAnalysisDf = ProductAnalysis.withColumn("rating_category",
      when($"rating">=4,"High")
        .when($"rating" >=3 && $"rating"<4,"Medium")
        .otherwise("Low")
    )
    ProductAnalysisDf.show()

    //Filter reviews where product_name starts with 'S'.

    val ProductStartsWithS = ProductAnalysisDf.filter($"product_name".startsWith("S"))
    ProductStartsWithS.show()

// Calculate the total count of reviews and average rating for each rating_category.

    val aggregationdf = ProductAnalysisDf.groupBy("rating_category")
      .agg(
        count("*").as("Total_Reviews"),
        avg("rating").as("Avg_Rating")
      )

    aggregationdf.show()
  }

}
