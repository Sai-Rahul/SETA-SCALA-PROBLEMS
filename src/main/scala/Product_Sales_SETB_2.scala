import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, max, min, month, sum, to_date, when}

object Product_Sales_SETB_2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Product_Sales_SETB_2")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.memory.executor","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data

    val Sales = Seq(
      (1,"Widget",700,"2024-01-15"),
      (2,"Gadget",150,"2024-01-20"),
      (3,"Widget",350,"2024-02-15"),
      (4,"Device",600,"2024-02-20"),
      (5,"Widget",100,"2024-03-05"),
      (6,"Gadget",500,"2024-03-12")
    ).toDF("sale_id","product_name","sale_amount","sale_date")

    val SalesWithCategory = Sales.withColumn("sale_category",
      when($"sale_amount" > 500,"High")
        .when($"sale_amount">= 200 && $"sale_amount" <=500,"Medium")
        .otherwise("Low")

    )
    SalesWithCategory.show()

    // Filter sales where product_name ends with 't'.
    val productStartsWithT = SalesWithCategory.filter($"product_name".startsWith("T"))

    productStartsWithT.show()
    // Calculate the total (sum), average (avg), maximum (max), and minimum (min) sale_amount
    //for each month.

    val salesWithMonth = Sales.withColumn("SalesMonth",to_date($"sale_date","yyyy-MM-dd"))
      .withColumn("month",month($"SalesMonth"))
    salesWithMonth.show()

    val aggregation = salesWithMonth.groupBy("month")
      .agg(sum($"sale_amount").alias("Total Sale Amount"),
        avg($"sale_amount").alias("Avg Sale"),
        max($"sale_amount").alias("Max Sale"),
        min($"sale_amount").alias("Min Sale"))

    aggregation.show()



  }

}
