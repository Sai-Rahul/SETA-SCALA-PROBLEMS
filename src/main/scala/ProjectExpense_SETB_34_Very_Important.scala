import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, max, min, month, sum, to_date, when}

object ProjectExpense_SETB_34_Very_Important {
  def main(args:Array[String]) : Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","ProjectExpense_SETB_34")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._

    val ProjectExpense = Seq(

      (1,"Development Project",8000,"2024-09-01"),
      (2,"Development Plan",4500,"2024-09-05"),
      (3,"Marketing Campaign",2500,"2024-09-10"),
      (4,"Development Phase",3000,"2024-09-15"),
      (5,"Development Task",10000,"2024-09-20"),
      (6,"R&D Project",1500,"2024-09-25")
    ).toDF("expense_id","project_name","expense_amount","expense_date")
    ProjectExpense.show()

    val ProjectExpDf = ProjectExpense.withColumn("expense_type",
      when($"expense_amount">7000,"High")
        .when($"expense_amount">=3000 && $"expense_amount"<=7000, "Medium")
        .otherwise("Low")
    )

    ProjectExpDf.show()

    val FilterProjectdf = ProjectExpDf.filter($"project_name".contains("Development"))
    FilterProjectdf.show()

    // Create a new column transaction_month that extracts the month from expense_date.

    val ExpMonthCast = ProjectExpense.withColumn("ExpenseDate",to_date($"expense_date","yyyy-MM-dd"))
      .withColumn("Expense_Month",month($"ExpenseDate"))

    ExpMonthCast.show()

    //Filter expenses that occurred in the month of 'September'.
    val SeptTransaction =ExpMonthCast.filter($"Expense_Month"===1)

    SeptTransaction.show()

    val aggrgationdf = ProjectExpDf.groupBy("expense_type")
      .agg(sum($"expense_amount").as("Total_Expense"),
        avg($"expense_amount").as("Avg_Expense"),
        min($"expense_amount").as("Minimum_Expense"),
        max($"expense_amount").as("Maximum_Expense")

      )
    aggrgationdf.show()


  }

}
