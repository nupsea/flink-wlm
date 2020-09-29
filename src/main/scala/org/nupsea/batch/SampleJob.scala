package org.nupsea.batch

import org.apache.flink.api.java.ExecutionEnvironment
import collection.JavaConverters._

object SampleJob {

  def main(args: Array[String]): Unit = {
    try {
      val env = ExecutionEnvironment.getExecutionEnvironment

      val trees = List("Mango", "Coconut", "Guava", "Neem")

      val dsTrees = env.fromCollection(trees.asJava)

      println(s" '|`'|` Tree Count: ${dsTrees.count()}")
    } catch {
      case e : Exception =>
        print("Error processing Job")
      }
    }

}
