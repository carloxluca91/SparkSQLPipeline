package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column

case class Alias(override val expression: String)
  extends SingleColumnSqlFunction(expression, SqlCatalog.Alias) {

  private val alias: String = group(3)
  override protected val transformationFunction: Column => Column = _.as(alias)

  override protected def asString: String = s"$nestedFunction.$functionName('$alias')"
}
