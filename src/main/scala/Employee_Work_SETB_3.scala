import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{avg, max, min, sum, when}

object Employee_Work_SETB_3 {

  def main(args : Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name","Employee_Work_SETB_3")
    sparkconf.set("spark.master","local[*]")
    sparkconf.set("spark.executor.memory","2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val Employee = Seq(
      (1,"2024-01-10",9,"Sales"),
      (2,"2024-01-11",7,"Support"),
      (3,"2024-01-12",8,"Sales"),
      (4,"2024-01-13",10,"Marketing"),
      (5,"2024-01-14",5,"Sales"),
      (6,"2024-01-15",6,"Support")

    ).toDF("employee_id","work_date","hours_worked","Department")

    val hoursDF = Employee.withColumn("hours_category",
      when($"hours_worked" > 8,"Overtime")
        .otherwise("Regular")

    )
    hoursDF.show()

    // Filter work hours where department starts with 'S'.

    val DepartmentWithS = hoursDF.filter($"Department".startsWith("S"))

    DepartmentWithS.show()

    //Calculate the total (sum), average (avg), maximum (max), and minimum (min) hours_worked
    //for each department.

    val aggregationsDF = hoursDF.groupBy($"Department")
      .agg(sum($"hours_worked").alias("Total_Work_hours"),
        avg($"hours_worked").alias("average_Work_hours"),
        max($"hours_worked").alias("max_work_hours"),
        min($"hours_worked").alias("min_work_hours")
      )

    aggregationsDF.show()
  }

}
