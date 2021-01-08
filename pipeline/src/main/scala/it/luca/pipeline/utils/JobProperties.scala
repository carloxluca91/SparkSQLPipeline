package it.luca.pipeline.utils

import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.log4j.Logger

case class JobProperties(private val propertiesConfiguration: PropertiesConfiguration) {

  def containsKey(key: String): Boolean = propertiesConfiguration.containsKey(key)

  def getString(key: String): String = propertiesConfiguration.getString(key)
}

object JobProperties {

  private val log = Logger.getLogger(getClass)

  def apply(propertiesFileName: String): JobProperties = {

    // Initialize PropertiesConfiguration object holding Spark application properties
    val builder = new FileBasedConfigurationBuilder(classOf[PropertiesConfiguration])
      .configure(new Parameters().properties().setFileName(propertiesFileName))
    val jobProperties = JobProperties(builder.getConfiguration)
    log.info(s"Successfully loaded .properties file '$propertiesFileName'")
    jobProperties
  }
}
