package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.StaticColumnExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class Col(override val expression: String)
  extends StaticColumnExpression(expression, Catalog.Col) {

  val columnName: String = group(2)

  override def getColumn: Column = col(columnName)

  override protected def asString: String = s"$functionName($columnName)"
}
