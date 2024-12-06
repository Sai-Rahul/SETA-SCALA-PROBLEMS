import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, max, min, month, sum, to_date, when, year}

object SalespersonPerformance_SETB_43_Very_Important {

  def main(args:Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","SalespersonPerformance")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val SalesPerson = Seq(
      (1,2500,"2024-12-01"),
      (2,15000,"2024-12-05"),
      (3,8000,"2024-12-10"),
      (4,12000,"2024-12-15"),
      (5,18000,"2024-12-20"),
      (6,5000,"2024-12-25")
    ).toDF("salesperson_id","sales_amount","sales_date")

    SalesPerson.show()

    val SalesPersonPerfDf = SalesPerson.withColumn("performance_category",
      when($"sales_amount">20000,"Top Performer")
    .when($"sales_amount">=10000 && $"sales_amount"<=20000,"Average Performer")
        .otherwise("Low Performer")
    )
    SalesPersonPerfDf.show()

    // Filter records where sales_date is in 'December 2024'.

    //convert Sales_date to DateType

    val SalesDateType = SalesPersonPerfDf.withColumn("sales_date",to_date($"sales_date","yyyy-MM-dd"))

    //filter for december 2024

    val filteredDF = SalesDateType.filter(year($"sales_date")===2024 && month($"sales_date")===12)

    filteredDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) sales_amount
    //for each performance_category.

    val aggregationDF = SalesPersonPerfDf.groupBy("performance_category")
      .agg(sum($"sales_amount").as("Total_Amount"),
        avg($"sales_amount").as("Average_Amount"),
        max($"sales_amount").as("Max_Amount"),
        min($"sales_amount").as("Min_Amount")

      )
    aggregationDF.show()
  }

}
