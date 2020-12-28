package it.luca.spark.sql.catalog.common

import it.luca.spark.sql.catalog.functions.SQLCatalog
import org.apache.spark.sql.Column

abstract class StaticColumnSQLFunction(override val expression: String,
                                       override val catalogExpression: SQLCatalog.Value)
  extends AbstractSQLFunction(expression, catalogExpression) {

  def getColumn: Column

}
