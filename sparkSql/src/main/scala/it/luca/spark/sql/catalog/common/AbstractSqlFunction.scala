package it.luca.spark.sql.catalog.common

import it.luca.spark.sql.catalog.parser.SqlCatalog

import scala.util.matching.Regex.Match

abstract class AbstractSqlFunction(val expression: String,
                                   val catalogExpression: SqlCatalog.Value) {

  private final val regexMatch: Match = catalogExpression.regex
    .findFirstMatchIn(expression)
    .get

  final def group(i: Int): String = regexMatch.group(i)

  final val functionName: String = group(1)

  protected def asString: String

  override def toString: String = asString

}
