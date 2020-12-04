package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.SingleColumnExpression
import it.luca.pipeline.spark.utils.SparkUtils
import org.apache.spark.sql.Column

case class Cast(override val expression: String)
  extends SingleColumnExpression(expression, Catalog.Cast) {

  private val dataTypeStr: String = group(3)

  override protected val transformationFunction: Column => Column = _.cast(SparkUtils.dataType(dataTypeStr))

  override protected def asString: String = s"$nestedFunction.$functionName('$dataTypeStr')"
}
