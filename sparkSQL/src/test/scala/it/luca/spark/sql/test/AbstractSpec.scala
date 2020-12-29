package it.luca.spark.sql.test

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should

import scala.reflect.runtime.universe._

abstract class AbstractSpec extends AnyFlatSpec with should.Matchers {

  def className[T](implicit typeTag: TypeTag[T]): String = typeOf[T].typeSymbol.name.toString

}
