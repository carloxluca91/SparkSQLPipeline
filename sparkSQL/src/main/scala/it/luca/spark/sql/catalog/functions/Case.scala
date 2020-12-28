package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.UnboundedColumnSQLFunction
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.spark.sql.Column

case class Case(override val expression: String)
  extends UnboundedColumnSQLFunction(expression, SQLCatalog.Case, subExpressionGroupIndex = 2) {

  private val otherWiseColumn: Column = SQLFunctionParser.parse(group(3))

  override protected def asString: String = {

    val indexedCasesStr: String = subExpressions
      .zipWithIndex
      .map(t => {
        val (s, i) = t
        s"# ${i + 1} -> $s"
      }).mkString(", ")

    s"$functionName { $indexedCasesStr }"
  }

  override protected val combiningFunction: Seq[Column] => Column =
    s => {
      s.foldRight(otherWiseColumn)((c1, c2) => {
        c1.otherwise(c2)
      })
    }
}
