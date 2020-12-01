package it.luca.pipeline.spark.etl.parsing

object EtlExpression extends Enumeration {

  protected case class Val(regex: scala.util.matching.Regex) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToETLExpressionVal(x: Value): Val = x.asInstanceOf[Val]

  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)
  val CurrentDateOrTimestamp: Val = Val("^(current_date|current_timestamp)\\(\\)$".r)
  val EqualToOrNotEqualTo: Val = Val("^(equalTo|notEqualTo)\\((\\w+\\(.+\\)), (\\w+\\(.+\\))\\)".r)
  val Lit: Val = Val("^(lit)\\(('?.+'?)\\)$".r)
  val ToDateOrTimestamp: Val = Val("^(to_date|to_timestamp)\\((\\w+\\(.*\\)), '(.+)'\\)$".r)

}
