package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.TwoColumnSQLFunction
import org.apache.spark.sql.Column

case class Compare(override val expression: String)
  extends TwoColumnSQLFunction(expression, SQLCatalog.Compare) {

  override protected def asString: String = s"$firstExpression.$functionName($secondExpression)"

  override protected val combiningFunction: (Column, Column) => Column = {

    // Resolve comparing operator depending on function name
    val functionNameLC: String = functionName.toLowerCase

    if (functionNameLC startsWith "less") {
      if (functionNameLC endsWith "equal") _ <= _ else _ < _

    } else if (functionNameLC startsWith "greater") {
      if (functionNameLC endsWith "equal") _ >= _ else _ > _

    } else {
      if (functionNameLC startsWith "not") _ =!= _ else _ === _
    }
  }
}
