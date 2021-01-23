package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.UnboundedColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat_ws

case class ConcatWs(override val expression: String)
  extends UnboundedColumnSqlFunction(expression, SqlCatalog.ConcatWs, subExpressionGroupIndex = 3) {

  private val separator: String = group(2)

  override protected val combiningFunction: Seq[Column] => Column = s => concat_ws(separator, s: _*)

  override protected def asString: String = s"$functionName(${subExpressions.mkString(", ")}, separator = '$separator')"
}
