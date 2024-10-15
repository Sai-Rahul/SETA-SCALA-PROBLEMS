import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{avg, sum, when,min, max}

object EmployeeAgeAndSalaryAnalysis_3 {
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
      (1,"John",28,60000),
      (2,"Jane",32,75000),
      (3,"Mike",45,75000),
      (4,"Alice",55,90000),
      (5,"Steve",62,110000),
      (6,"ClaireJ",40,40000)
    ).toDF("employee_id","name","age","salary")

    val age_groupDF = employees.withColumn("age_group",
      when($"age" <30,"Young")
      .when($"age" <=50,"Mid")
        .otherwise("Senior")
    )

    age_groupDF.show()

    val SalaryBasedDf = age_groupDF.withColumn("salary_range",
      when($"salary" >100000,"High")
        .when($"salary" >=50000,"Medium")
        .otherwise("Low")

    )
SalaryBasedDf.show()

    // Filter employees whose name starts with 'J'.

    val EmployeeNameStartsWithJ = SalaryBasedDf.filter($"name".startsWith("J"))
    // Filter employees whose name ends with 'e'

    val EmployeeNameEndssWithJ = SalaryBasedDf.filter($"name".endsWith("J"))

    EmployeeNameStartsWithJ.show()
    EmployeeNameEndssWithJ.show()

//Calculate the total (sum), average (avg), maximum (max),
    // and minimum (min) salary for each age_group.

    val aggregationdf = age_groupDF.groupBy($"age_group")
      .agg(sum($"salary").alias("total_salary"),
        avg($"salary").alias("average_salary"),
        max($"salary").alias("maximum_salary"),
        min($"salary").alias("maximum_salary")
      )

    aggregationdf.show()


  }
}

