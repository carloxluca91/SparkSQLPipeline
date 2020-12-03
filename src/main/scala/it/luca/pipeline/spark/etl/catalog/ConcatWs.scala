package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.UnboundedColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat_ws

case class ConcatWs(override val expression: String)
  extends UnboundedColumnExpression(expression, Catalog.ConcatWs, subExpressionGroupIndex = 3) {

  private val separator: String = group(2)

  override protected val combiningFunction: Seq[Column] => Column = s => concat_ws(separator, s: _*)

  override protected def asString: String = s"$functionName(${subExpressions.mkString(", ")}, separator = '$separator')"
}
