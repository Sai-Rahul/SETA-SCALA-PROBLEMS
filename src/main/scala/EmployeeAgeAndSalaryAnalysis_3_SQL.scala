import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object EmployeeAgeAndSalaryAnalysis_3_SQL {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "EmployeeAgeAndSalaryAnalysis_3")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data

    val employees = Seq(
      (1, "John", 28, 60000),
      (2, "Jane", 32, 75000),
      (3, "Mike", 45, 75000),
      (4, "Alice", 55, 90000),
      (5, "Steve", 62, 110000),
      (6, "ClaireJ", 40, 40000)
    ).toDF("employee_id", "name", "age", "salary")

    employees.createOrReplaceTempView("employees")

    val age_groupDF = spark.sql(
      """
         SELECT *,
         CASE
         WHEN age <30 THEN "Young"
         WHEN age <=50 THEN "Mid"
         ELSE "Senior"
         END AS age_group
         FROM employees
          """)

    val SalaryBasedDf = spark.sql(
      """
         SELECT *,
         CASE
         WHEN salary > 100000 THEN "High"
         WHEN salary >=50000 THEN "Medium"
         ELSE "Low"
         END AS salary_range
         FROM employees
        """)
    age_groupDF.show()
    age_groupDF.createOrReplaceTempView("age_groupDF")
    SalaryBasedDf.show()
    // Filter employees whose name starts with 'J'.

    val EmployeeNameStartsWithJ = spark.sql(
      """
         SELECT *
         FROM employees
         WHERE name LIKE 'J%'
        """)
    EmployeeNameStartsWithJ.show()

    // Filter employees whose name ends with 'e'

    val EmployeeNameEndsWithJ = spark.sql(
      """
         SELECT *
         FROM employees
         WHERE name LIKE '%J'
       """)

    EmployeeNameEndsWithJ.show()

    //Calculate the total (sum), average (avg), maximum (max),
    // and minimum (min) salary for each age_group.

    val aggregationdf = spark.sql(
      """
         SELECT age_group,
         SUM(SALARY) AS TOTAL_SALARY,
         AVG(SALARY) AS AVERAGE_SALARY,
         MIN(SALARY) AS MIN_SALARY,
         MAX(SALARY) AS MAX_SALARY
         FROM age_groupDF
         GROUP BY age_group

        """)
    aggregationdf.show()


  }
}
