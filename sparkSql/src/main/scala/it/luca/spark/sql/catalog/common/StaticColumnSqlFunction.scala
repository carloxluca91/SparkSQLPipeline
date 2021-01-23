package it.luca.spark.sql.catalog.common

import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column

abstract class StaticColumnSqlFunction(override val expression: String,
                                       override val catalogExpression: SqlCatalog.Value)
  extends AbstractSqlFunction(expression, catalogExpression) {

  def getColumn: Column

}
