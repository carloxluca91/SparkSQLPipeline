package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSQLFunction
import org.apache.spark.sql.Column

case class Alias(override val expression: String)
  extends SingleColumnSQLFunction(expression, SQLCatalog.Alias) {

  private val alias: String = group(3)
  override protected val transformationFunction: Column => Column = _.alias(alias)

  override protected def asString: String = s"$nestedFunction.$functionName('$alias')"
}
