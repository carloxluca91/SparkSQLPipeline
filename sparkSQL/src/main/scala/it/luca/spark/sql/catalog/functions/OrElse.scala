package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.TwoColumnSQLFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.coalesce

case class OrElse(override val expression: String)
  extends TwoColumnSQLFunction(expression, SQLCatalog.OrElse) {

  override protected val combiningFunction: (Column, Column) => Column = coalesce(_, _)

  override protected def asString: String = s"$firstExpression.$functionName($secondExpression)"
}
