package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.SingleColumnExpression
import org.apache.spark.sql.Column

case class IsNullOrIsNotNull(override val expression: String)
  extends SingleColumnExpression(expression, Catalog.IsNullOrIsNotNull) {

  override protected val transformationFunction: Column => Column =
    if (functionName.toLowerCase contains "not") _.isNotNull else _.isNull

  override protected def asString: String = s"$nestedFunction.$functionName()"
}
