package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.StaticColumnSQLFunction
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{current_date, current_timestamp}

case class CurrentDateOrTimestamp(override val expression: String)
  extends StaticColumnSQLFunction(expression, SQLCatalog.CurrentDateOrTimestamp) {

  override def getColumn: Column = if (functionName.toLowerCase endsWith "date") current_date else current_timestamp

  override protected def asString: String = s"$functionName()"
}
