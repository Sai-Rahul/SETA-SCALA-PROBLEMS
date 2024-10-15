import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Student_Grade_Analysis_SQL {

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

    students.createOrReplaceTempView("students")
    val studentsWithGrade = spark.sql(
      """
         SELECT *,
         CASE
         WHEN score >90 THEN "A"
         WHEN score >80 THEN "B"
         WHEN score >70 THEN "C"
         WHEN score >60 THEN "D"
         ELSE "F"
         END As Grade
         FROM students

        """)
    studentsWithGrade.show()

    studentsWithGrade.createOrReplaceTempView("studentsWithGrade")
  // Calculate the average score per subject.
    val avg = spark.sql(
      """
         SELECT subject, AVG(score)
         FROM studentsWithGrade
         GROUP BY subject
        """)

    avg.show()

    //Find the maximum and minimum score per subject.

    val MaxAndMinDF = spark.sql(
      """
         SELECT subject,
          MIN(score) AS Minimum_Score,
          MAX(score) As Maximum_Score
          FROM studentsWithGrade
          GROUP BY subject

        """)

    MaxAndMinDF.show()

    //Count the number of students in each grade category per subject.

    val CountStudents = spark.sql(
      """
         SELECT grade, subject,
         COUNT(*) As student_count
         FROM studentsWithGrade
         GROUP BY grade, subject
         ORDER BY grade, subject

        """)

    CountStudents.show()




  }
}


