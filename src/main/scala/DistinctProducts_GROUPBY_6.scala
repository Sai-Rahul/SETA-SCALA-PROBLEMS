import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{countDistinct, sum}

object DistinctProducts_GROUPBY_6 {
  def main(arg: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "DistinctProducts_GROUPBY_6")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    val purchaseData = Seq(
      ("Customer1", "Product1", 100),
      ("Customer1", "Product2", 150),
      ("Customer1", "Product3", 200),
      ("Customer2", "Product2", 120),
      ("Customer2", "Product3", 180),
      ("Customer3", "Product1", 80),
      ("Customer3", "Product3", 250)
    ).toDF("Customer", "Product", "Amount")

    purchaseData.show()
//// Group by Customer and calculate the count of distinct products

    val distinctProducts = purchaseData.groupBy($"Customer")
      .agg(countDistinct($"Product").alias("DistinctCount"))

    distinctProducts.show()

    // Group by Customer and calculate the sum of Amount

    val SumAmount = purchaseData.groupBy("Customer")
      .agg(sum($"Amount").alias("Total_AMount"))

    SumAmount.show()

  }
}