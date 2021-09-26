package org.nupsea.flink.stream.kinesis

import org.apache.flink.api.java.utils.ParameterTool
import org.nupsea.flink.utils.ConFileReader
import org.slf4j.{Logger, LoggerFactory}

object TreeStreamer {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val pt = ParameterTool.fromArgs(args)
    logger.info(s"App args: ${pt.getProperties}")

    val vars = ConFileReader.readMyVars(pt.get("vars"))
    val con = ConFileReader.readWorkflowConf(pt.get("appConf"), vars)
    logger.info(s"Config variables: $con")

  }

}
