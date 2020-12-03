package it.luca.pipeline.spark.etl.catalog

object Catalog extends Enumeration {

  protected case class Val(regex: scala.util.matching.Regex) extends super.Val

  import scala.language.implicitConversions

  implicit def valueToETLExpressionVal(x: Value): Val = x.asInstanceOf[Val]

  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)
  val Compare: Val = Val("^(equal|notEqual|greater|greaterOrEqual|less|lessOrEqual)\\((\\w+\\(.*\\)), (\\w+\\(.*\\))\\)$".r)
  val Concat: Val = Val("^(concat)\\((.+\\))\\)$".r)
  val ConcatWs: Val = Val("^(concatWs)\\('(.+)', (.+)\\)$".r)
  val CurrentDateOrTimestamp: Val = Val("^(currentDate|currentTimestamp)\\(\\)$".r)
  val IsNullOrIsNotNull: Val = Val("^(isNull|isNotNull)\\((\\w+\\(.*\\))\\)$".r)
  val Lit: Val = Val("^(lit)\\(('?.+'?)\\)$".r)
  val ToDateOrTimestamp: Val = Val("^(toDate|toTimestamp)\\((\\w+\\(.*\\)), '(.+)'\\)$".r)

}