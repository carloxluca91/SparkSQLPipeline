package it.luca.pipeline.utils

import it.luca.pipeline.exception.UnExistingKeyException
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger

import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe._

case class JobProperties(propertiesFile: String) {

  private final val logger = Logger.getLogger(classOf[JobProperties])

  val jobProperties = new PropertiesConfiguration()
  jobProperties.load(propertiesFile)
  logger.info("Successfully loaded job .properties file")

  def containsKey(key: String): Boolean = jobProperties.containsKey(key)

  def get(key: String): String = {

    if(containsKey(key)) {

      val keyValue = jobProperties.getString(key)
      logger.info(s"Value of property $key is $keyValue")
      keyValue
    } else {
      throw UnExistingKeyException(key)
    }
  }

  def getOrElse(keyOpt: Option[String], defaultValue: String): String = {

    if (keyOpt.nonEmpty) {
      val key: String = keyOpt.get
      Try {get(key)} match {
        case Failure(_) =>
          logger.warn(s"Key '$key' is not present within .properties file. Thus, returning the default value ($defaultValue)")
          defaultValue
        case Success(value) => value
      }
    } else {
      logger.warn(s"Input key is null. Thus, returning the default value ($defaultValue)")
      defaultValue
    }
  }

  def getAs[T: TypeTag](key: String): T = {

    val f: String => Any = if (typeOf[T] <:< typeOf[Boolean]) {
      s: String => s.toBoolean
    } else if (typeOf[T] <:< typeOf[Int]) {
      s: String => s.toInt
    } else identity

    val stringValue: String = get(key)
    f(stringValue).asInstanceOf[T]
  }

  def getOrElseAs[T](keyOpt: Option[String], defaultValue: T): T = {

    val defaultValueStr: String = defaultValue.toString
    val f: String => Any = defaultValue match {
      case _: Boolean => s => s.toBoolean
      case _: Int => s => s.toInt
      case _ => identity
    }

    f(getOrElse(keyOpt, defaultValueStr)).asInstanceOf[T]
  }
}
