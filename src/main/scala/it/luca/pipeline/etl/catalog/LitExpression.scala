package it.luca.pipeline.etl.catalog

import it.luca.pipeline.etl.common.StaticColumnExpression
import it.luca.pipeline.etl.parsing.EtlExpression
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import scala.util.matching.Regex

case class LitExpression(override val expression: String)
  extends StaticColumnExpression(expression, EtlExpression.Lit) {

  final val litValue: String = group(2)

  override def getColumn: Column = {

    val trueLiterValue: Any = if ((litValue startsWith "'") & (litValue endsWith "'")) {
      litValue.substring(1, litValue.length - 1)
    } else {

      val doubleLiteralValueRegex: Regex = "^\\d+\\.\\d+$".r
      doubleLiteralValueRegex.findFirstMatchIn(litValue) match {
        case None => litValue.toInt
        case Some(_) => litValue.toDouble
      }
    }

    lit(trueLiterValue)
  }

  override def asString: String = s"${functionName.toUpperCase}($litValue)"
}
