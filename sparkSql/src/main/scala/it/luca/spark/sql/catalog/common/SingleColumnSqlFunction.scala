package it.luca.spark.sql.catalog.common

import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column

abstract class SingleColumnSqlFunction(override val expression: String,
                                       override val catalogExpression: SqlCatalog.Value)
  extends AbstractSqlFunction(expression, catalogExpression) {

  final val nestedFunction: String = group(2)
  protected val transformationFunction: Column => Column

  def getColumn(inputColumn: Column): Column = transformationFunction(inputColumn)

}
