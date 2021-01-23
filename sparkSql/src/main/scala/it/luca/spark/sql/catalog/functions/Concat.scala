package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.UnboundedColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.concat

case class Concat(override val expression: String)
  extends UnboundedColumnSqlFunction(expression, SqlCatalog.Concat, subExpressionGroupIndex = 2) {

  override protected val combiningFunction: Seq[Column] => Column = s => concat(s: _*)
}
