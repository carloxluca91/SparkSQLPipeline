package it.luca.pipeline.spark.etl.parsing

import it.luca.pipeline.spark.etl.catalog._
import it.luca.pipeline.test.AbstractSpec
import org.apache.spark.sql.{Column, functions}

class CatalogExpressionParserSpec extends AbstractSpec {

  private val (c1ColName, c2ColName) = ("c1", "c2")
  private val (c1Col, c2Col) = (functions.col(c1ColName), functions.col(c2ColName))
  private val (c1ColStr, c2ColStr) = (s"col('$c1ColName')", s"col('$c2ColName')")

  s"An EtlExpressionParser object" should
    s"correctly parse ${className[Col]} expression" in {

    val col = CatalogExpressionParser.parse(c1ColStr)
    assert(col == c1Col)
  }

  it should
    s"correctly parse ${className[Lit]} expression" in {

    val literalValueStr = "literal"
    val litStrCol = CatalogExpressionParser.parse(s"lit('$literalValueStr')")
    assert(litStrCol == functions.lit(literalValueStr))

    val literalValueInt = 33
    val litInt = CatalogExpressionParser.parse(s"lit($literalValueInt)")
    assert(litInt == functions.lit(33))

    val literalValueDouble = 33.3
    val litDouble = CatalogExpressionParser.parse(s"lit($literalValueDouble)")
    assert(litDouble == functions.lit(literalValueDouble))
  }

  it should
    s"correctly parse ${className[CurrentDateOrTimestamp]} expression" in {

    ("Date" -> functions.current_date) ::
      ("Timestamp" -> functions.current_timestamp) :: Nil foreach {
      t =>
        val (functionSuffix, expectedColumn): (String, Column) = t
        val currentDateOrTimestamp = CatalogExpressionParser.parse(s"current$functionSuffix()")
        assert(currentDateOrTimestamp == expectedColumn)
    }
  }

  it should
    s"correctly parse ${className[ToDateOrTimestamp]} expression" in {

    val format = "yyyy-MM-dd"
    val testSeq: Seq[(String, (Column, String) => Column)] = Seq(
      "Date" -> functions.to_date,
      "Timestamp" -> functions.to_timestamp)

    testSeq foreach {
      t =>
        val (functionSuffix, expectedFunction): (String, (Column, String) => Column) = t
        val toDateOrTimestamp = CatalogExpressionParser.parse(s"to$functionSuffix($c1ColStr, '$format')")
        assert(toDateOrTimestamp == expectedFunction(c1Col, format))
    }
  }

  it should
    s"correctly parse ${className[Compare]} expression" in {

    val lit1Str = "lit(1)"
    val testSeq: Seq[(String, Column)] =
      ("equal", c1Col === 1) ::
        ("notEqual", c1Col =!= 1) ::
        ("greater", c1Col > 1) ::
        ("greaterOrEqual", c1Col >= 1) ::
        ("less", c1Col < 1) ::
        ("lessOrEqual", c1Col <= 1) :: Nil

    testSeq foreach {
      t =>
        val (functionName, expectedColumn) = t
        val parsedColumn = CatalogExpressionParser.parse(s"$functionName($c1ColStr, $lit1Str)")
        assert(parsedColumn == expectedColumn)
    }
  }

  it should
    s"correctly parse ${className[Concat]} expression" in {

    val expectedColumn: Column = functions.concat(c1Col, c2Col)
    val actualColumn: Column = CatalogExpressionParser.parse(s"concat($c1ColStr, $c2ColStr)")
    assert(actualColumn == expectedColumn)
  }

  it should
    s"correctly parse ${className[ConcatWs]} expression" in {

    val separator = "-"
    val expectedColumn: Column = functions.concat_ws(separator, c1Col, c2Col)
    val actualColumn: Column = CatalogExpressionParser.parse(s"concatWs('$separator', $c1ColStr, $c2ColStr)")
    assert(actualColumn == expectedColumn)
  }

  it should
    s"correctly parse ${className[IsNullOrIsNotNull]} expression" in {

    val testSeq = ("isNull", c1Col.isNull) :: ("isNotNull", c1Col.isNotNull) :: Nil
    testSeq foreach {
      t =>
        val (functionName, expectedColumn) = t
        val actualColumn = CatalogExpressionParser.parse(s"$functionName($c1ColStr)")
        assert(actualColumn == expectedColumn)
    }
  }
}
