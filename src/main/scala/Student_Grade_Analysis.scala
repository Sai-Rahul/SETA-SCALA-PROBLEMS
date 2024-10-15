import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, count, max, min, sum, when}

object Student_Grade_Analysis {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.name", "Student_Grade_Analysis")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._
    // Creating a DataFrame with sample data
    val students = Seq(
      (1, "Alice", 95, "Math"),
      (2, "Bob", 85, "Math"),
      (3, "Charlie", 72, "Science"),
      (4, "David", 68, "Science"),
      (5, "Eva", 59, "Math"),
      (6, "Frank", 90, "Science"),
      (7, "Grace", 88, "Math"),
      (8, "Henry", 75, "Science")
    ).toDF("student_id", "name", "score", "subject")

    val studentsWithGrades = students.withColumn("grade",
      when(col("score") >= 90,"A")
        .when(col("score") >=80,"B")
        .when(col("score") >= 70,"C")
        .when(col("score") >= 60,"D")
        .otherwise("F")

    )

    studentsWithGrades.show()
 // Calculate the average score per subject.

    val AverageScorePerSubject = studentsWithGrades.groupBy("subject")
      .agg(sum(col("score")))

    AverageScorePerSubject.show()

    // Find the maximum and minimum score per subject.

    val MaxMinScorePerSubject = studentsWithGrades.groupBy("subject")
      .agg(max(col("score")).alias("Max_Score"),
          min(col("score")).alias("Min_Score"))

    MaxMinScorePerSubject.show()
    //Count the number of students in each grade category per subject.

    val CountStudents = studentsWithGrades.groupBy("subject","grade")
      .agg(count("student_id"))
      .orderBy("subject","grade")


    CountStudents.show()



  }
}
