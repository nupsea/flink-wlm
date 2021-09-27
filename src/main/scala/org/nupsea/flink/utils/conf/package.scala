package org.nupsea.flink.utils

import java.util.Properties

package object conf {

  type ConMap = Map[String, AnyRef]

  type LogData = Map[String, Any]

  type IndexData = Map[String, Any]

  type SrcConf = List[Map[String, String]]

  trait BaseCon {
    val name: String
    val `type`: String
    val props: ConMap
    val meta: ConMap
  }

  class InvalidConfException(message: String) extends Exception(message)

  case class MyVar(
                    value: String,
                    `type`: String = "text",
                    properties: Map[String, String] = Map.empty
                  )

  case class FlinkConf(
                        name: String = "Flink-App",
                        checkpointEnabled: Boolean = false,
                        checkpointLocation: String,
                        checkpointIntervalMinutes: Long = 20L,
                        parallelism: Option[Int]
                      )


  case class DataSrc(
                      name: String,
                      `type`: String,
                      props: ConMap,
                      meta: ConMap = Map.empty
                    ) extends BaseCon

  case class DataTgt(name: String,
                     `type`: String,
                     props: ConMap,
                     meta: ConMap = Map.empty
                    ) extends BaseCon

  case class WorkflowCon(flink: FlinkConf,
                         src: Option[List[DataSrc]],
                         tgt: Option[List[DataTgt]]
                        )


  implicit class ConfHelper(c: BaseCon) {
    def asJavaProperties: Properties = c.props.foldLeft(new Properties())((p, v) => {
      p.setProperty(v._1, v._2 match {
        case s: String => s
        case _ => v.toString
      })
      p
    })

    def asMapOfStrings: Map[String, String] = ConfHelper.convertMapAnyToMapString(c.props)

    def getListOfStringsFromMeta(fieldName: String): List[String] = (c.meta.get(fieldName) match {
      case Some(arr: Traversable[_]) => arr.map(_.toString).toList
      case Some(s: String) => s.split(',').map(_.trim).toList
      case _ => throw new InvalidConfException(s"Invalid property `$fieldName` in the config ${c.name} ")
    }).filter(!_.equals(""))


    def getMapOfStrings(fieldName: String): Map[String, String] = c.props.get(fieldName) match {
      case Some(m: Map[String, _]) => ConfHelper.convertMapAnyToMapString(m)
      case _ => throw new InvalidConfException(s"Invalid property `$fieldName` in the config ${c.name} ")
    }

    def getListOfStrings(fieldName: String): List[String] = (c.props.get(fieldName) match {
      case Some(arr: Traversable[_]) => arr.map(_.toString).toList
      case Some(s: String) => s.split(',').map(_.trim).toList
      case _ => throw new InvalidConfException(s"Invalid property `$fieldName` in the config ${c.name}]")
    }).filter(!_.equals(""))

    def getString(fieldName: String): String = c.props.get(fieldName) match {
      case Some(arr: Traversable[_]) => throw new InvalidConfException(s"Invalid property `$fieldName` in the config ${c.name} ")
      case Some(s: String) => s
      case Some(v) => v.toString
      case None => throw new InvalidConfException(s"Property `$fieldName` is not found in the config ${c.name} ")
    }

    def getOption[T](fieldName: String)(implicit parser: Any => T = (x: Any) => x.asInstanceOf[T]): Option[T] = c.props.get(fieldName) match {
      case None => None
      case Some(v: T) => Some(v)
      case Some(v: Any) => Some(parser(v))
    }

    def getOrElse[T](fieldName: String, default: T)(implicit parser: Any => T = (x: Any) => x.asInstanceOf[T]): T = c.props.get(fieldName) match {
      case None => default
      case Some(v: T) => v
      case Some(v: Any) => parser(v)
    }
  }


  object ConfHelper {
    def convertMapAnyToMapString(raw: Map[String, Any]): Map[String, String] = raw.map(f => f._2 match {
      case _: Traversable[_] => throw new InvalidConfException(s"Invalid property [$f._1] found in $raw")
      case s: String => f._1 -> s
      case v => f._1 -> v.toString
    })
  }


  object AWSConfigUtil {
    def getConsumerConfig(propMap: Map[String, String]): Properties = {
      val consumerConfig = new Properties()
      propMap.foreach(x => consumerConfig.put(x._1, x._2))
      consumerConfig
    }
  }

}

