package it.luca.pipeline.spark.etl.parsing

import it.luca.pipeline.spark.etl.catalog.{Col, CurrentDateOrTimestamp, IsEqualOrIsNotEqual, Lit, ToDateOrTimestamp}
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

    ("date" -> functions.current_date) ::
      ("timestamp" -> functions.current_timestamp) :: Nil foreach {
      t =>
        val (functionSuffix, expectedColumn): (String, Column) = t
        val currentDateOrTimestamp = EtlExpressionParser.parse(s"current_$functionSuffix()")
        assert(currentDateOrTimestamp == expectedColumn)
    }
  }

  it should
    s"correctly parse ${className[ToDateOrTimestamp]} expression" in {

    val inputColumn = functions.col("c1")
    val format = "yyyy-MM-dd"

    val testSeq: Seq[(String, (Column, String) => Column)] = Seq(
      "date" -> functions.to_date,
      "timestamp" -> functions.to_timestamp)

    testSeq foreach {
      t =>
        val (functionSuffix, expectedFunction): (String, (Column, String) => Column) = t
        val toDateOrTimestamp = EtlExpressionParser.parse(s"to_$functionSuffix(col('c1'), '$format')")
        assert(toDateOrTimestamp == expectedFunction(inputColumn, format))
    }
  }

  it should
    s"correctly parse ${className[IsEqualOrIsNotEqual]} expression" in {

    val inputColumnName = "c1"
    val (colC1, lit1) = (s"col('$inputColumnName')", "lit(1)")
    val testSeq: Seq[(String, Column)] =
      ("isEqual", functions.col(inputColumnName) === 1) ::
        ("isNotEqual", functions.col(inputColumnName) =!= 1) :: Nil

    testSeq foreach {
      t =>
        val (functionName, expectedColumn) = t
        val parsedColumn = EtlExpressionParser.parse(s"$functionName($colC1, $lit1)")
        assert(parsedColumn == expectedColumn)
    }
  }
}
