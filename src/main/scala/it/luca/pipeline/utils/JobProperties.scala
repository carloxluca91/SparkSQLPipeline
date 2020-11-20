package it.luca.pipeline.utils

import java.io.FileInputStream

import it.luca.pipeline.exception.UnExistingKeyException
import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger

case class JobProperties(propertiesFile: String) {

  private final val logger = Logger.getLogger(classOf[JobProperties])

  val jobProperties = new PropertiesConfiguration()
  jobProperties.load(new FileInputStream(propertiesFile))
  logger.info("Successfully loaded job .properties file")

  def containsKey(key: String): Boolean = jobProperties.containsKey(key)

  @throws[UnExistingKeyException]
  def get(key: String): String = getAs[String](key)

  def getOrElse(keyOpt: Option[String], defaultValue: String): String = getOrElseAs[String](keyOpt, defaultValue)

  @throws[UnExistingKeyException]
  def getAs[T](key: String): T = {

    val propertyValueOpt: Option[String] = Option(jobProperties.getString(key))
    val returnValue: T = propertyValueOpt match {
      case None => throw UnExistingKeyException(key)
      case Some(x) => x.asInstanceOf[T]
    }

    logger.info(s"Value of property $key = $returnValue")
    returnValue
  }

  def getOrElseAs[T](keyOpt: Option[String], defaultValue: T): T = {

    keyOpt match {
      case None =>

        logger.warn(s"Input key is null. Thus, returning the default value ($defaultValue)")
        defaultValue

      case Some(key) =>

        if (jobProperties.containsKey(key)) {

          val returnValue: T = jobProperties.getString(key).asInstanceOf[T]
          logger.info(s"Value of property $key is $returnValue")
          returnValue

        } else {

          logger.warn(s"Key '$key' is not present within .properties file. Thus, returning the default value ($defaultValue)")
          defaultValue
        }
    }
  }
}
