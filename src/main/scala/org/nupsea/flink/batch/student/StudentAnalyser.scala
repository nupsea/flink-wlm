package org.nupsea.flink.batch.student

import com.typesafe.sslconfig.ssl.debug.FixLoggingAction
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment, createTypeInformation}


object StudentAnalyser {

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    /**
     * 1.
     * Compute a total score of students for each student record.
     * Total score equals class score plus test score.
     */

    val ds: DataSet[StudentScore] = env.readCsvFile[StudentScore](
      "src/main/resources/DATA/student_scores.csv",
      ignoreFirstLine = true)

    val dsWithTotal: DataSet[Map[String, Any]] = ds.map(r => {
      Map(
        "student" -> r.student,
        "subject" -> r.subject,
        "classScore" -> r.classScore,
        "testScore" -> r.testScore,
        "totalScore" -> (r.classScore + r.testScore)
      )
    })

    dsWithTotal.print()


    /**
     * 2.
     * Print the total score for each student for Physics subject.
     * Print only the student name and the total score for physics.
     */

    val phyTotalScore = dsWithTotal
      .filter(
        r => r("subject").toString.equalsIgnoreCase("physics"))
      .map(r => s" ---> ${r("student")} scored ${r("totalScore")} ")

    phyTotalScore.print()


    /**
     * 3.
     * Compute average of the total scores across all subjects for each student.
     */

    // Note: reduce doesn't work on Map data as of Flink 1.11 version. Needs to be `Tuple` or a `Case Class`.
    //            .groupBy(_("student").toString)
    //            .reduce {
    //              (r1, r2) =>  Map(r1.getOrElse("student", "").toString -> (r1.getOrElse("totalScore", 0.0) + r2.getOrElse("totalScore", 0.0).toString))
    //            }

    val stuScore: DataSet[(String, Float)] = dsWithTotal
      .map(r => (r("student").toString, r("totalScore").asInstanceOf[Float]))


    val stuAvg = stuScore.groupBy(0).reduce {
      (r1, r2) => (r1._1, r1._2 + r2._2)
    }
    stuAvg.print()


    /**
     * 4.
     * Find the student with the highest score in each subject.
     * Essentially, find the top student by subject.
     */
    // Note: Project, to selectively index tuples isn't supported in Scala as of Flink 1.11

    val stuSubScore: DataSet[(String, String, Float)] = dsWithTotal
      .map(r => (r("student").toString, r("subject").toString, r("totalScore").asInstanceOf[Float]))

    val highScorerPerSub = stuSubScore.groupBy(1).max(2)
    highScorerPerSub.print()

  }

}
