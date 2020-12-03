package it.luca.pipeline.spark.etl.common

import it.luca.pipeline.spark.etl.catalog.EtlExpression

import scala.util.matching.Regex.Match

abstract class AbstractExpression(val expression: String,
                                  val etlExpression: EtlExpression.Value) {

  private final val regexMatch: Match = etlExpression.regex
    .findFirstMatchIn(expression)
    .get

  final def group(i: Int): String = regexMatch.group(i)

  final val functionName: String = group(1)

  protected def asString: String

  override def toString: String = asString

}
