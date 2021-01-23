package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.StaticColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class Col(override val expression: String)
  extends StaticColumnSqlFunction(expression, SqlCatalog.Col) {

  val columnName: String = group(2)

  override def getColumn: Column = col(columnName)

  override protected def asString: String = s"$functionName($columnName)"
}
