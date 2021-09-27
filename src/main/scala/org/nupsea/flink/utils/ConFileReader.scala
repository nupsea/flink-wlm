package org.nupsea.flink.utils

import com.typesafe.config.ConfigFactory
import org.apache.flink.kinesis.shaded.com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import org.apache.flink.kinesis.shaded.com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3URI}
import org.nupsea.flink.utils.conf.{BaseCon, InvalidConfException, MyVar, MyVarBuilder, WorkflowCon}
import com.typesafe.config.{Config, ConfigFactory, ConfigObject => TypesafeConfigObject, ConfigValue => TypesafeConfigValue}
import pureconfig.generic.ProductHint

import java.net.URL
import scala.io.{BufferedSource, Source => IOSource}
import pureconfig.{CamelCase, ConfigFieldMapping, ConfigReader, ConfigSource}

import scala.collection.JavaConverters._
import pureconfig.generic.auto._

import java.util



object ConFileReader {

  implicit def hint[T]: ProductHint[T] = ProductHint[T](ConfigFieldMapping(CamelCase, CamelCase))

  def readMyVars(filePath: String): Map[String, String] = {
    val stringToMyVarReader = ConfigReader[String].map(MyVar(_))
    implicit val myVarReader = ConfigReader[MyVar].orElse(stringToMyVarReader)

    val rawVars: Map[String, MyVar] = ConfigSource.string(readFile(filePath)).loadOrThrow[Map[String,MyVar]]
    rawVars.mapValues(MyVarBuilder.apply).map(identity)
  }

  def getSourceStream(fPath: String): BufferedSource = {
    val srcStream: BufferedSource = if (fPath.startsWith("s3://")) {
      import org.apache.flink.kinesis.shaded.com.amazonaws.services.s3.model.S3Object

      val s3Client = AmazonS3ClientBuilder
        .standard()
        .withCredentials(new DefaultAWSCredentialsProviderChain())
        .build()
      val uri: AmazonS3URI = new AmazonS3URI(fPath)
      val s3Object: S3Object = s3Client.getObject(uri.getBucket, uri.getKey)

      IOSource.fromInputStream(s3Object.getObjectContent)
    } else {
      val pattern = """^(\w+):/""".r.pattern
      IOSource.fromURL(
        if (pattern.matcher(fPath).matches()) new URL(fPath)
        else getClass.getResource(fPath)
      )
    }
    srcStream
  }

  def readWorkflowConf(pipelineFile: String, variables: Map[String, String]): WorkflowCon = {
    def toScalaCollection(raw: AnyRef): AnyRef = raw match {
      case m: util.Map[_, AnyRef] => m.asScala.mapValues(toScalaCollection).map(identity)
      case l: util.List[AnyRef] => l.asScala.map(toScalaCollection)
      case i: java.lang.Iterable[_] => throw new InvalidConfException(s"Unsupported Data type : ${i.getClass}")
      case _ => raw
    }

    implicit val mapReader: ConfigReader[Map[String, AnyRef]] =
      pureconfig.ConfigReader[TypesafeConfigObject].map(
        co => co.unwrapped().asScala.mapValues(toScalaCollection).toMap)

    ConfigSource.fromConfig(
      ConfigFactory.parseString(readFile(pipelineFile))
        .resolveWith(ConfigFactory.parseMap(variables.asJava))
    ).loadOrThrow[WorkflowCon]
  }

  def readFile(filePath: String): String = {
    val fConfig: BufferedSource = getSourceStream(filePath)
    val contents = fConfig.getLines().mkString
    fConfig.close()
    contents
  }


}
