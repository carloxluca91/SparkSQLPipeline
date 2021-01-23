package it.luca.spark.sql.catalog.common

import it.luca.spark.sql.catalog.functions.Concat
import it.luca.spark.sql.test.AbstractSpec

class UnboundedColumnSQLFunctionSpec extends AbstractSpec {

  private def assertOk(subExpressions: Seq[String]): Unit = {

    val s = s"concat(${subExpressions.mkString(", ")})"
    val concat = Concat(s)

    assert(concat.subExpressions.length == subExpressions.length)
    concat.subExpressions.zip(subExpressions) foreach {
        case(actual, expected) => assert(actual == expected)
    }
  }

  s"A ${className[UnboundedColumnSqlFunction]} object" should
    "correctly detect the provided subexpressions (simple case)" in {

    val columnNames = "c1" :: "c2" :: "c3" :: Nil
    val subExpressions = columnNames
      .map(x => s"col('$x')")
    assertOk(subExpressions)
  }

  it should "correctly detect the provided subexpressions (mixed case)" in {

    val subExpressions = "substring(col('c1'), 0, 2)" :: "col('c2')" :: "col('c3')" :: Nil
    assertOk(subExpressions)
  }

  it should "correctly detect the provided subexpressions (complex case)" in {

    val subExpressions = "substring(col('c1'), 0, 2)" :: "lpad(col('c2'), 10, 'X')" :: "dateFormat(col('c3'), 'yyyy-MM-dd')" :: Nil
    assertOk(subExpressions)
  }
}
