package it.luca.spark.sql.catalog.common

import it.luca.spark.sql.catalog.functions.SQLCatalog
import org.apache.spark.sql.Column

abstract class SingleColumnSQLFunction(override val expression: String,
                                       override val catalogExpression: SQLCatalog.Value)
  extends AbstractSQLFunction(expression, catalogExpression) {

  final val nestedFunction: String = group(2)
  protected val transformationFunction: Column => Column

  def getColumn(inputColumn: Column): Column = transformationFunction(inputColumn)

}
