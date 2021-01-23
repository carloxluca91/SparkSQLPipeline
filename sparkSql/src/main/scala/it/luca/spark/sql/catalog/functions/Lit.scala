package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.StaticColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.lit

import scala.util.matching.Regex

case class Lit(override val expression: String)
  extends StaticColumnSqlFunction(expression, SqlCatalog.Lit) {

  private val litValue: String = group(2)

  override def getColumn: Column = {

    // If it is a string, we have to cut off quotes
    val trueLiteralValue: Any = if ((litValue startsWith "'") & (litValue endsWith "'")) {
      litValue.substring(1, litValue.length - 1)
    } else {

      // otherwise, it can be a number (double or int) or null
      val doubleLiteralValueRegex: Regex = "^\\d+\\.\\d+$".r
        doubleLiteralValueRegex.findFirstMatchIn(litValue) match {
        case None => if (litValue == "null") null else litValue.toInt
        case Some(_) => litValue.toDouble
      }
    }

    lit(trueLiteralValue)
  }

  override protected def asString: String = s"$functionName($litValue)"
}
