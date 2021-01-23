package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.TwoColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.when

case class When(override val expression: String)
  extends TwoColumnSqlFunction(expression, SqlCatalog.When) {

  override protected def asString: String = s"$functionName($firstExpression) then $secondExpression)"

  override protected val combiningFunction: (Column, Column) => Column = when(_, _)
}
