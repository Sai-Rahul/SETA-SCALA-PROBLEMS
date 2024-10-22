import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, max, min, month, sum, to_date, when, year}

object Event_Sponsorship_SETB_28 {

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

    EventSponsorshipDF.show()

    val AmountCategoryDF = EventSponsorshipDF.withColumn("amount_category",
      when($"sponsorship_amount" >10000,"High")
        .when($"sponsorship_amount">=5000 && $"sponsorship_amount"<=10000,"Medium")
        .otherwise("Low")
    )
    AmountCategoryDF.show()

    // Filter sponsorships where sponsorship_date is in 'February 2024'.

    val SponsorshipDateConvertDF = EventSponsorshipDF.withColumn("CovertDate",to_date($"sponsorship_date","yyyy-MM-dd"))
    //SponsorshipDateConvertDF.show()
    val SponsorshipFiltrationDF = SponsorshipDateConvertDF.filter(month($"CovertDate")===2 && year($"CovertDate") === 2024)
    SponsorshipFiltrationDF.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min)
    //sponsorship_amount for each amount_category

    val aggregationDF = AmountCategoryDF.groupBy("amount_category")
      .agg(sum($"sponsorship_amount").alias("Total_amount"),
        avg($"sponsorship_amount").alias("Average_Amount"),
        max($"sponsorship_amount").alias("Maximum_Amount"),
        min($"sponsorship_amount").alias("Minimum_Amount"))

    aggregationDF.show()


  }
}
