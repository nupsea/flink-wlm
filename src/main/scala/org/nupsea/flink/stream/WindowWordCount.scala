package org.nupsea.flink.stream

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time


object WindowWordCount {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.socketTextStream("localhost", 9999)

    val counts = data.flatMap { _.toLowerCase.split("\\W+") filter {_.nonEmpty} }
      .map { (_, 1) }
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
      .sum(1)

    counts.print(" -- ")

    env.execute("WordCount_with_Windowing")

  }

}
