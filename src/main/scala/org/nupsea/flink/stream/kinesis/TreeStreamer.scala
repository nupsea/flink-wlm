package org.nupsea.flink.stream.kinesis

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.{Configuration, GlobalConfiguration}
import org.apache.flink.contrib.streaming.state.{EmbeddedRocksDBStateBackend, RocksDBStateBackend}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer
import org.nupsea.flink.utils.ConFileReader
import org.nupsea.flink.utils.conf.{AWSConfigUtil, ConfHelper}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit

object TreeStreamer {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {


    val pt = ParameterTool.fromArgs(args)
    logger.info(s"App args: ${pt.getProperties}")
    val env = pt.get("env", "local")

    val vars = ConFileReader.readMyVars(pt.get("vars"))
    val appConf = ConFileReader.readWorkflowConf(pt.get("appConf"), vars)
    logger.info(s"App Config variables: $appConf")
    val flinkConf = appConf.flink

    // Setup streaming execution environment
    implicit val seEnv: StreamExecutionEnvironment = if (env.equals("local")) {
      val c = new Configuration
      val gc = GlobalConfiguration.loadConfiguration(c)
      StreamExecutionEnvironment.createLocalEnvironment(flinkConf.parallelism.getOrElse(1), gc)
    } else
      StreamExecutionEnvironment.getExecutionEnvironment

    // seEnv.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)


    // Setup check-pointing with RocksDB StateBackend
    if (flinkConf.checkpointEnabled) seEnv
      .setStateBackend(new EmbeddedRocksDBStateBackend(false))
      .enableCheckpointing(Time.of(flinkConf.checkpointIntervalMinutes, TimeUnit.MINUTES).toMilliseconds,
        CheckpointingMode.AT_LEAST_ONCE)

    val cn_conf = appConf.src.map(f => f.filter(_.name == "TreesConsumer")).get.head
    val kc_conf = AWSConfigUtil.getConsumerConfig(ConfHelper.convertMapAnyToMapString(cn_conf.props))

    val fkc = new FlinkKafkaConsumer[String](
      cn_conf.meta("consumerTopic").toString,
      new SimpleStringSchema(),
      kc_conf)
    val dataStream:DataStream[String] = seEnv.addSource(fkc)

    val pr_conf = appConf.tgt.map(f => f.filter(_.name == "TreesProducer")).get.head
    val kp_conf = AWSConfigUtil.getConsumerConfig(ConfHelper.convertMapAnyToMapString(pr_conf.props))

    val producer = new FlinkKinesisProducer[String](
      new SimpleStringSchema(),
      kp_conf
    )
    producer.setDefaultStream(pr_conf.props("topicName").toString)
    producer.setFailOnError(true)
    producer.setDefaultPartition("0")

    dataStream.addSink(producer).name("KinProducer")

    seEnv.execute(flinkConf.name)
  }

}
