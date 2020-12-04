package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.SingleColumnExpression
import org.apache.spark.sql.Column

case class Alias(override val expression: String)
  extends SingleColumnExpression(expression, Catalog.Alias) {

  private val alias: String = group(3)
  override protected val transformationFunction: Column => Column = _.alias(alias)

  override protected def asString: String = s"$nestedFunction.$functionName('$alias')"
}
