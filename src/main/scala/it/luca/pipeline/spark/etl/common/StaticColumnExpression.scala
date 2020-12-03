package it.luca.pipeline.spark.etl.common
import it.luca.pipeline.spark.etl.catalog.Catalog
import org.apache.spark.sql.Column

abstract class StaticColumnExpression(override val expression: String,
                                      override val catalogExpression: Catalog.Value)
  extends AbstractExpression(expression, catalogExpression) {

  def getColumn: Column

}
