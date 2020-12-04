package it.luca.pipeline.spark.etl.catalog

import it.luca.pipeline.spark.etl.common.UnboundedColumnExpression
import it.luca.pipeline.spark.etl.parsing.CatalogParser
import org.apache.spark.sql.Column

case class Case(override val expression: String)
  extends UnboundedColumnExpression(expression, Catalog.Case, subExpressionGroupIndex = 2) {

  private val otherWiseColumn: Column = CatalogParser.parse(group(3))

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
