import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, count, when}

object Product_Rating_SETB_7 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Product_Rating_SETB_7")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val Product_Rating = Seq(
      (1,"Smartphone",4,"2024-01-15"),
      (2,"Speaker",3,"2024-01-20"),
      (3,"Smartwatch",5,"2024-02-15"),
      (4,"Screen",2,"2024-02-20"),
      (5,"Speakers",4,"2024-03-05"),
      (6,"Soundbar",3,"2024-03-12")
    ).toDF("review_id","product_name","rating","review_date")

    Product_Rating.show()

    val category = Product_Rating.withColumn("rating_category",
      when($"rating">=4,"High")
      .when($"rating">=3 && $"rating"<4, "Medium")
        .otherwise("Low"))
    category.show()

    // Filter reviews where product_name starts with 'S'.

    val ProductStartsWithS = category.filter($"product_name".startsWith("S"))
    ProductStartsWithS.show()

    // Calculate the total count of reviews and average rating for each rating_category.

    val aggregationDF = category.groupBy($"rating_category")
      .agg(count($"rating").alias("rating"),
      avg($"rating").alias("avergae_rating"))

    aggregationDF.show()



  }
}
