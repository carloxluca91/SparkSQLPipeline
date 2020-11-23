package it.luca.pipeline.etl.expression

object EtlExpression extends Enumeration {

  protected case class Val(regex: scala.util.matching.Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToETLExpressionVal(x: Value): Val = x.asInstanceOf[Val]

  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)

}
