package it.luca.pipeline.spark.etl.common
import it.luca.pipeline.spark.etl.catalog.Catalog
import org.apache.spark.sql.Column

abstract class SingleColumnExpression(override val expression: String,
                                      override val catalogExpression: Catalog.Value)
  extends AbstractExpression(expression, catalogExpression) {

  final val nestedFunction: String = group(2)
  protected val transformationFunction: Column => Column

  def getColumn(inputColumn: Column): Column =  transformationFunction(inputColumn)

}
