import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Employee_Work_SETB_3_SQL {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Employee_Work_SETB_3")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val Employee = Seq(
      (1, "2024-01-10", 9, "Sales"),
      (2, "2024-01-11", 7, "Support"),
      (3, "2024-01-12", 8, "Sales"),
      (4, "2024-01-13", 10, "Marketing"),
      (5, "2024-01-14", 5, "Sales"),
      (6, "2024-01-15", 6, "Support")

    ).toDF("employee_id", "work_date", "hours_worked", "Department")

    Employee.createOrReplaceTempView("Employee")


    val hoursDF = spark.sql(
      """SELECT
         employee_id,work_date,hours_worked,Department,
         CASE
         WHEN hours_worked > 8 THEN "OverTime"
         ELSE "Regular"
         END AS hours_category
         FROM Employee


        """)

    hoursDF.show()

    // Filter work hours where department starts with 'S'.

    val DepartmentWithS = spark.sql(
      """
         SELECT *
         FROM Employee
         WHERE Department LIKE 'S%'
       """)
    DepartmentWithS.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) hours_worked
    //for each department.

    val aggregationDF = spark.sql(
      """SELECT Department,
         SUM(hours_worked) As Total_hours,
         AVG(hours_worked) As Average_hours,
         MAX(hours_worked) As max_hours,
         MIN(hours_worked) As min_hours
         FROM Employee
         GROUP BY Department
        """)
    aggregationDF.show()

  }
}
