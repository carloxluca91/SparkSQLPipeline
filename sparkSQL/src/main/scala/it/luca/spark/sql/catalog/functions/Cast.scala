package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSQLFunction
import it.luca.spark.sql.utils.DataTypeUtils
import org.apache.spark.sql.Column

case class Cast(override val expression: String)
  extends SingleColumnSQLFunction(expression, SQLCatalog.Cast) {

  private val dataTypeStr: String = group(3)

  override protected val transformationFunction: Column => Column = _.cast(DataTypeUtils.dataType(dataTypeStr))

  override protected def asString: String = s"$nestedFunction.$functionName('$dataTypeStr')"
}
