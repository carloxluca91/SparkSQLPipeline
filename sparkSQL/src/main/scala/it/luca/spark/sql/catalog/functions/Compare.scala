package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.TwoColumnSQLFunction
import org.apache.spark.sql.Column

case class Compare(override val expression: String)
  extends TwoColumnSQLFunction(expression, SQLCatalog.Compare) {

  override protected def asString: String = s"$firstExpression.$functionName($secondExpression)"

  override protected val combiningFunction: (Column, Column) => Column = {

    // Resolve comparing operator depending on function name
    functionName match {
      case "eq" => _ === _
      case "neq" => _ =!= _
      case "gt" => _ > _
      case "geq" => _ >= _
      case "lt" => _ < _
      case "leq" => _ <= _
    }
  }
}
