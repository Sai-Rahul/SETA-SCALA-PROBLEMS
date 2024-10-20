import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, max, min, sum, when}

object Product_Inventory_SETB_4 {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Product_Inventory_SETB_4")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val ProductInventoryDF = Seq(
      (1,"Pro Widget",30,"2024-01-10"),
      (2,"Pro Device",120,"2024-01-15"),
      (3,"Standard",200,"2024-01-20"),
      (4,"Pro Gadget",40,"2024-02-01"),
      (5,"Standard",60,"2024-02-10"),
      (6,"Pro Device",90,"2024-03-01")
    ).toDF("product_id","product_name","stock","last_restocked")

    ProductInventoryDF.show()

    val StockStatus = ProductInventoryDF.withColumn("stock_status",
      when($"stock"<50, "Low")
        .when($"stock">=50 && $"stock" <=150, "Medium")
        .otherwise("High")
    )
    StockStatus.show()

    // Filter products where product_name contains 'Pro'.

    val ProductStartWithPRO = StockStatus.filter($"product_name".contains("Pro"))

    ProductStartWithPRO.show()

    // Calculate the total (sum), average (avg), maximum (max), and minimum (min) stock for each
    //stock status

    val aggregationDF = StockStatus.groupBy("stock_status")
      .agg(sum($"stock").alias("Total_stock"),
        avg($"stock").alias("Average_stock"),
        max($"stock").alias("Maximum_stock"),
        min($"stock").alias("Minimum_stock")

      )
    aggregationDF.show()

  }

}
