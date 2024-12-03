import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, max, min, sum, to_date, when}
import org.apache.log4j.{Level, Logger}

object UtilityBill_SETB_21 {

  def main(args:Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","UtilityBill_SETB_21")
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

    UtilityBillDF.show()

    val UtilityBills = UtilityBillDF.withColumn("bill_status",
      when($"bill_amount">200,"High")
        .when($"bill_amount">=100 && $"bill_amount"<=200,"Medium")
        .otherwise("low")

    )
    UtilityBills.show()

    // Filter bills where billing_date is in 'February 2024'.

    val DateConvDF = UtilityBills.filter(to_date($"billing_date","yyyy-MM-dd")
    .between("2024-02-01","2024-02-29"))

    DateConvDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) bill_amount
    //for each bill_status

    val aggregationDF = UtilityBills.groupBy("bill_status")
      .agg(sum($"bill_amount").as("Total_Amount"),
      avg($"bill_amount").as("Avg_bill_amount"),
        max($"bill_amount").as("max_bill_amount"),
        min($"bill_amount").as("min_bill_amount")

      )

    aggregationDF.show()




  }

}
