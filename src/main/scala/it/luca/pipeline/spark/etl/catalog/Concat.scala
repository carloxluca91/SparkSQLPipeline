package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.UnboundedColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat

case class Concat(override val expression: String)
  extends UnboundedColumnExpression(expression, Catalog.Concat, subExpressionGroupIndex = 2) {

  override protected val combiningFunction: Seq[Column] => Column = s => concat(s: _*)
}
