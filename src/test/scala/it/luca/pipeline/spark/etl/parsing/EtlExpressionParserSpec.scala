package it.luca.pipeline.spark.etl.parsing

import it.luca.pipeline.spark.etl.catalog._
import it.luca.pipeline.test.AbstractSpec
import org.apache.spark.sql.{Column, functions}

class EtlExpressionParserSpec extends AbstractSpec {

  s"An EtlExpressionParser object" should
    s"correctly parse ${className[Col]} expression" in {

    val expectedColumnName = "c1"
    val col = EtlExpressionParser.parse(s"col('$expectedColumnName')")
    assert(col == functions.col(expectedColumnName))
  }

  it should
    s"correctly parse ${className[Lit]} expression" in {

    val literalValueStr = "literal"
    val litStrCol = EtlExpressionParser.parse(s"lit('$literalValueStr')")
    assert(litStrCol == functions.lit(literalValueStr))

    val literalValueInt = 33
    val litInt = EtlExpressionParser.parse(s"lit($literalValueInt)")
    assert(litInt == functions.lit(33))

    val literalValueDouble = 33.3
    val litDouble = EtlExpressionParser.parse(s"lit($literalValueDouble)")
    assert(litDouble == functions.lit(literalValueDouble))
  }

  it should
    s"correctly parse ${className[CurrentDateOrTimestamp]} expression" in {

    ("Date" -> functions.current_date) ::
      ("Timestamp" -> functions.current_timestamp) :: Nil foreach {
      t =>
        val (functionSuffix, expectedColumn): (String, Column) = t
        val currentDateOrTimestamp = EtlExpressionParser.parse(s"current$functionSuffix()")
        assert(currentDateOrTimestamp == expectedColumn)
    }
  }

  it should
    s"correctly parse ${className[ToDateOrTimestamp]} expression" in {

    val inputColumn = functions.col("c1")
    val format = "yyyy-MM-dd"

    val testSeq: Seq[(String, (Column, String) => Column)] = Seq(
      "Date" -> functions.to_date,
      "Timestamp" -> functions.to_timestamp)

    testSeq foreach {
      t =>
        val (functionSuffix, expectedFunction): (String, (Column, String) => Column) = t
        val toDateOrTimestamp = EtlExpressionParser.parse(s"to$functionSuffix(col('c1'), '$format')")
        assert(toDateOrTimestamp == expectedFunction(inputColumn, format))
    }
  }

  it should
    s"correctly parse ${className[Compare]} expression" in {

    val inputColumnName = "c1"
    val (colC1Str, lit1Str) = (s"col('$inputColumnName')", "lit(1)")
    val colC1: Column = functions.col(inputColumnName)
    val testSeq: Seq[(String, Column)] =
      ("equal", colC1 === 1) ::
        ("notEqual", colC1 =!= 1) ::
        ("greater", colC1 > 1) ::
        ("greaterOrEqual", colC1 >= 1) ::
        ("less", colC1 < 1) ::
        ("lessOrEqual", colC1 <= 1) :: Nil

    testSeq foreach {
      t =>
        val (functionName, expectedColumn) = t
        val parsedColumn = EtlExpressionParser.parse(s"$functionName($colC1Str, $lit1Str)")
        assert(parsedColumn == expectedColumn)
    }
  }

  it should
    s"correctly parse ${className[Concat]} expression" in {

    val (c1ColName, c2ColName) = ("c1", "c2")
    val expectedColumn: Column = functions.concat(functions.col(c1ColName), functions.col(c2ColName))
    val actualColumn: Column = EtlExpressionParser.parse(s"concat(col('$c1ColName'), col('$c2ColName'))")
    assert(actualColumn == expectedColumn)
  }

  it should
    s"correctly parse ${className[ConcatWs]} expression" in {

    val (c1ColName, c2ColName) = ("c1", "c2")
    val separator = "-"
    val expectedColumn: Column = functions.concat_ws(separator, functions.col(c1ColName), functions.col(c2ColName))
    val actualColumn: Column = EtlExpressionParser.parse(s"concatWs('$separator', col('$c1ColName'), col('$c2ColName'))")
    assert(actualColumn == expectedColumn)
  }
}
