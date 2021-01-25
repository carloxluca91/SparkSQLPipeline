package it.luca.pipeline.utils

import org.apache.commons.configuration.PropertiesConfiguration
import org.apache.log4j.Logger

import java.io.InputStream

case class JobProperties(private val propertiesConfiguration: PropertiesConfiguration) {

  def containsKey(key: String): Boolean = propertiesConfiguration.containsKey(key)

  def getString(key: String): String = propertiesConfiguration.getString(key)
}

object JobProperties {

  private val log = Logger.getLogger(getClass)

  def apply(propertiesFileName: String): JobProperties = {

    val jobProperties = JobProperties(new PropertiesConfiguration(propertiesFileName))
    log.info(s"Successfully loaded .properties file '$propertiesFileName'")
    jobProperties
  }

  def apply(inputStream: InputStream): JobProperties = {

    val propertiesConfiguration = new PropertiesConfiguration()
    propertiesConfiguration.load(inputStream)
    JobProperties(propertiesConfiguration)
  }
}
