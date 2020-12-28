package it.luca.spark.sql.catalog.common

import it.luca.spark.sql.catalog.functions.SQLCatalog
import org.apache.spark.sql.Column

abstract class TwoColumnSQLFunction(override val expression: String,
                                    override val catalogExpression: SQLCatalog.Value)
  extends AbstractSQLFunction(expression, catalogExpression) {

  final val firstExpression: String = group(2)
  final val secondExpression: String = group(3)

  protected val combiningFunction: (Column, Column) => Column

  def getColumn(firstColumn: Column, secondColumn: Column): Column = combiningFunction(firstColumn, secondColumn)
}
