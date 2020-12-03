package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.TwoColumnExpression
import org.apache.spark.sql.Column

case class Compare(override val expression: String)
  extends TwoColumnExpression(expression, Catalog.Compare) {

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
