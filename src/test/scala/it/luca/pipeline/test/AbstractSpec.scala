package it.luca.pipeline.test

import org.apache.commons.configuration.PropertiesConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe._

abstract class AbstractSpec extends AnyFlatSpec with should.Matchers {

  final val jobProperties: PropertiesConfiguration = new PropertiesConfiguration("spark_application.properties")

  def className[T](implicit typeTag: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString
}
