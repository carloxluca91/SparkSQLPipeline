package it.luca.pipeline.spark.etl.common
import it.luca.pipeline.spark.etl.catalog.Catalog
import org.apache.spark.sql.Column

abstract class TwoColumnExpression(override val expression: String,
                                   override val catalogExpression: Catalog.Value)
  extends AbstractExpression(expression, catalogExpression) {

  final val firstExpression: String = group(2)
  final val secondExpression: String = group(3)

  protected val combiningFunction: (Column, Column) => Column

  override protected def asString: String = s"$firstExpression.$functionName($secondExpression)"

  def getColumn(firstColumn: Column, secondColumn: Column): Column = combiningFunction(firstColumn, secondColumn)
}
