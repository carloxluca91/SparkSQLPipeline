package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.TwoColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when

case class When(override val expression: String)
  extends TwoColumnExpression(expression, Catalog.When) {

  override protected def asString: String = s"$functionName($firstExpression) then $secondExpression)"

  override protected val combiningFunction: (Column, Column) => Column = when(_, _)
}
