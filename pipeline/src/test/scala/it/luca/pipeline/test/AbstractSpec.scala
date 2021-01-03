package it.luca.pipeline.test

import org.apache.commons.configuration2.PropertiesConfiguration
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder
import org.apache.commons.configuration2.builder.fluent.Parameters
import org.apache.log4j.Logger
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe._

abstract class AbstractSpec extends AnyFlatSpec with should.Matchers {

  private val log = Logger.getLogger(getClass)
  private val jobPropertiesFile = "spark_application.properties"

  private val builder = new FileBasedConfigurationBuilder(classOf[PropertiesConfiguration])
    .configure(new Parameters().properties().setFileName(jobPropertiesFile))
  final val jobProperties: PropertiesConfiguration = builder.getConfiguration

  log.info(s"Successfully loaded '$jobPropertiesFile' file")

  def className[T](implicit typeTag: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString
}
