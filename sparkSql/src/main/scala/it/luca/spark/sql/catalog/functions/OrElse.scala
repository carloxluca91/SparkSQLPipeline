package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.TwoColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.coalesce

case class OrElse(override val expression: String)
  extends TwoColumnSqlFunction(expression, SqlCatalog.OrElse) {

  override protected val combiningFunction: (Column, Column) => Column = coalesce(_, _)

  override protected def asString: String = s"$firstExpression.$functionName($secondExpression)"
}
