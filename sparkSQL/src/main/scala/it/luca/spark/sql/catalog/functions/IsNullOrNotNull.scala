package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSQLFunction
import org.apache.spark.sql.Column

case class IsNullOrNotNull(override val expression: String)
  extends SingleColumnSQLFunction(expression, SQLCatalog.IsNullOrIsNotNull) {

  override protected val transformationFunction: Column => Column =
    if (functionName.toLowerCase contains "not") _.isNotNull else _.isNull

  override protected def asString: String = s"$nestedFunction.$functionName()"
}
