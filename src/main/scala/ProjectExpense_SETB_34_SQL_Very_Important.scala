import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ProjectExpense_SETB_34_SQL_Very_Important {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("akka").setLevel(Level.OFF)
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "ProjectExpense_SETB_34")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()


    import spark.implicits._

    val ProjectExpense = Seq(

      (1, "Development Project", 8000, "2024-09-01"),
      (2, "Development Plan", 4500, "2024-09-05"),
      (3, "Marketing Campaign", 2500, "2024-09-10"),
      (4, "Development Phase", 3000, "2024-09-15"),
      (5, "Development Task", 10000, "2024-09-20"),
      (6, "R&D Project", 1500, "2024-09-25")
    ).toDF("expense_id", "project_name", "expense_amount", "expense_date")
    ProjectExpense.createOrReplaceTempView("ProjectExpense")

    val ProjectExpDf = spark.sql(
      """
        SELECT
        expense_id,project_name,expense_amount,expense_date,
        CASE
        WHEN expense_amount>7000 THEN 'High'
        WHEN expense_amount>=3000 AND  expense_amount<=7000 THEN 'Medium'
        ELSE 'Low'
        END AS expense_type
        FROM ProjectExpense
        """)

    ProjectExpDf.createOrReplaceTempView("ProjectExpDf")


    // Filter expenses where project_name contains 'Development'.
    val FilterProjectdf = spark.sql(
      """
        SELECT *
        FROM ProjectExpense
        WHERE project_name LIKE '%Development%'
        """)
    FilterProjectdf.show()

    // Create a new column transaction_month that extracts the month from expense_date.

    val ExpMonthCast = spark.sql(
      """
         SELECT *,
         month(TO_DATE(expense_date,'yyyy-MM-dd')) AS Expense_Month
         FROM ProjectExpense
       """)
    ExpMonthCast.createOrReplaceTempView("ExpMonthCast")

    //Filter expenses that occurred in the month of 'September'.

    val FilterSeptember = spark.sql(
      """
        SELECT *
        FROM ExpMonthCast
        WHERE Expense_Month = 9
        """)

    FilterSeptember.show()

    val aggregationDF = spark.sql(
      """SELECT expense_type,
         SUM(expense_amount) AS Total_Sum,
         AVG(expense_amount) AS Average,
         MIN(expense_amount) AS Min_Exp,
         MAX(expense_amount) AS Max_Exp
        FROM ProjectExpDf
        GROUP BY expense_type

        """)

    aggregationDF.show()


  }
}
