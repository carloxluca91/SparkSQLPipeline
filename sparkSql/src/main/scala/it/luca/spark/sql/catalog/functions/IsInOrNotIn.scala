package it.luca.spark.sql.catalog.functions

import it.luca.spark.sql.catalog.common.SingleColumnSqlFunction
import it.luca.spark.sql.catalog.parser.SqlCatalog
import org.apache.spark.sql.Column

case class IsInOrNotIn(override val expression: String)
  extends SingleColumnSqlFunction(expression, SqlCatalog.IsInOrNotIn) {

  private val inList: Seq[String] = group(3)
    .split(", ")
    .map(s => if (s.startsWith("'") && s.endsWith("'")) s.substring(1, s.length - 1) else s)

  override protected val transformationFunction: Column => Column = c => {

    val inCondition = c.isin(inList: _*)
    if (functionName.toLowerCase.contains("not")) !inCondition else inCondition
  }

  override protected def asString: String = s"$nestedFunction.$functionName(${inList.mkString(", ")})"
}
