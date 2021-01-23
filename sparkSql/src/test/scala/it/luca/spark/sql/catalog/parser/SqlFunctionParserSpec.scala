package it.luca.spark.sql.catalog.parser

import it.luca.spark.sql.catalog.exceptions.UndefinedSQLFunctionException
import it.luca.spark.sql.catalog.functions._
import it.luca.spark.sql.test.AbstractSpec
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.scalatest.BeforeAndAfterAll

class SqlFunctionParserSpec extends AbstractSpec with BeforeAndAfterAll {

  private val (firstCol, firstColStr) = (col("firstCol"), s"col('firstCol')")
  private val (secondCol, secondColStr) = (col("secondCol"), s"col('secondCol')")

  private def assertEqualColumns(expectedColumn: Column, actualColumn: Column): Unit = {

    assert(expectedColumn.expr.semanticEquals(actualColumn.expr))
  }

  s"A ${SqlFunctionParser.getClass.getSimpleName} object" should
    s"throw a ${className[UndefinedSQLFunctionException]} when an SQL function is not defined" in {

    val s = "ilBudello(col('c1'))"
    assertThrows[UndefinedSQLFunctionException](SqlFunctionParser.parse(s))
  }

  // Case
  it should s"correctly parse ${className[Case]} SQL function" in {

    val (threshold, gtLabel, ltLabel, eqLabel) = (0, ">", "<", "=")
    val expectedColumn = when(firstCol > threshold, gtLabel)
      .otherwise(when(firstCol < threshold, ltLabel).otherwise(eqLabel))

    val actualColumn = SqlFunctionParser.parse(sqlFunction =
      s"case(when(gt($firstColStr, lit($threshold)), lit('$gtLabel')), " +
      s"when(lt($firstColStr, lit($threshold)), lit('$ltLabel'))).otherWise(lit('$eqLabel'))")

   assertEqualColumns(expectedColumn, actualColumn)
  }

  // Cast
  it should s"correctly parse ${className[Cast]} SQL function" in {

    val testSeq = (DataTypes.LongType, "long") :: (DataTypes.StringType, "xyz") :: Nil
    testSeq foreach {
      case(expectedDType, typeString) =>
        val expected = firstCol.cast(expectedDType)
        val actual = SqlFunctionParser.parse(f"cast($firstColStr, '$typeString')")
        assertEqualColumns(expected, actual)
    }
  }

  // Col
  it should s"correctly parse ${className[Col]} SQL function" in {

    val actual = SqlFunctionParser.parse(firstColStr)
    assertEqualColumns(firstCol, actual)
  }

  // Compare
  it should s"correctly parse ${className[Compare]} SQL function" in {

    val lit1Str = "lit(1)"
    val testSeq: Seq[(String, Column)] =
      ("eq", firstCol === 1) ::
        ("neq", firstCol =!= 1) ::
        ("gt", firstCol > 1) ::
        ("geq", firstCol >= 1) ::
        ("lt", firstCol < 1) ::
        ("leq", firstCol <= 1) :: Nil

    testSeq foreach {
      case (functionName, expectedColumn) =>
        val actualColumn = SqlFunctionParser.parse(s"$functionName($firstColStr, $lit1Str)")
        assertEqualColumns(expectedColumn, actualColumn)
    }
  }

  // Concat
  it should s"correctly parse ${className[Concat]} SQL function" in {

    val expected = concat(firstCol, secondCol)
    val actual = SqlFunctionParser.parse(s"concat($firstColStr, $secondColStr)")
    assertEqualColumns(expected, actual)
  }

  // ConcatWs
  it should s"correctly parse ${className[ConcatWs]} SQL function" in {

    val separator = "-"
    val expected = concat_ws(separator, firstCol, secondCol)
    val actual = SqlFunctionParser.parse(s"concatWs('$separator', $firstColStr, $secondColStr)")
    assertEqualColumns(expected, actual)
  }

  // CurrentDateOrTimestamp
  it should s"correctly parse ${className[CurrentDateOrTimestamp]} SQL function" in {

    ("Date" -> current_date) :: ("Timestamp" -> current_timestamp) :: Nil foreach {
      case (functionSuffix, expected) =>
        val actual = SqlFunctionParser.parse(s"current$functionSuffix()")
        assertEqualColumns(expected, actual)
    }
  }

  // DateFormat
  it should s"correctly parse ${className[DateFormat]} SQL function" in {

    val format = "yyyy/MM/dd"
    val expected = date_format(firstCol, format)
    val actual = SqlFunctionParser.parse(s"dateFormat($firstColStr, '$format')")
    assertEqualColumns(expected, actual)
  }

  // IsInOrNotIn
  it should s"correctly parse ${className[IsInOrNotIn]} SQL function" in {

    val (one, two, three) = ("1", "2", "3")
    val inList = one :: two :: three :: Nil
    val inListString = s"'$one', '$two', '$three'"
    val testSeq = ("isIn", firstCol.isin(inList: _*)) :: ("isNotIn", !firstCol.isin(inList: _*)) :: Nil
    testSeq foreach {
      case (functionName, expectedColumn) =>
        val actualColumn = SqlFunctionParser.parse(s"$functionName($firstColStr, [$inListString])")
        assertEqualColumns(expectedColumn, actualColumn)
    }
  }


  // IsNullOrNotNull
  it should s"correctly parse ${className[IsNullOrNotNull]} SQL function" in {

    val testSeq = ("isNull", firstCol.isNull) :: ("isNotNull", firstCol.isNotNull) :: Nil
    testSeq foreach {
      case (functionName, expectedColumn) =>
        val actualColumn = SqlFunctionParser.parse(s"$functionName($firstColStr)")
        assertEqualColumns(expectedColumn, actualColumn)
    }
  }

  // LeftOrRightPad
  it should s"correctly parse ${className[LeftOrRightPad]} SQL function" in {

    val length = 10
    val pad = "0"
    val testSeq: Seq[(String, (Column, Int, String) => Column)] = ("l", lpad(_, _, _)) :: ("r", rpad(_, _, _)) :: Nil
    testSeq foreach {
      case (prefix, paddingFunction) =>
        val expected = paddingFunction(firstCol, length, pad)
        val actual = SqlFunctionParser.parse(s"${prefix}pad($firstColStr, $length, '$pad')")
        assertEqualColumns(expected, actual)
    }
  }

  // Lit
  it should s"correctly parse ${className[Lit]} SQL function" in {

    val testSeq = "literal" :: 33 :: 33.3 :: "null" :: Nil
    testSeq foreach {
      value =>
        val (litValue, litValueStr) = value match {
          case str: String => if (str == "null") (null, str) else (str, s"'$str'")
          case any: Any => (any, any)
        }

        val expected = lit(litValue)
        val actual = SqlFunctionParser.parse(s"lit($litValueStr)")
        assertEqualColumns(expected, actual)
    }
  }

  // LowerOrUpper
  it should s"correctly parse ${className[LowerOrUpper]} SQL function" in {

    val testSeq = ("lower", lower(_)) :: ("upper", upper(_)) :: Nil
    testSeq foreach {
      case (functionName, function) =>
        val expected = function(firstCol)
        val actual = SqlFunctionParser.parse(s"$functionName($firstColStr)")
        assertEqualColumns(expected, actual)
    }
  }

  // OrElse
  it should s"correctly parse ${className[OrElse]} SQL function" in {

    val testSeq = 1 :: "0,0" :: Nil
    testSeq foreach {
      v =>
        val expected = coalesce(firstCol, lit(v))
        val stringToParse = v match {
          case s: String => s"'$s'"
          case o: Any => o
        }

        val actual = SqlFunctionParser.parse(s"orElse($firstColStr, lit($stringToParse))")
        assertEqualColumns(expected, actual)
    }
  }

  // Replace
  it should s"correctly parse ${className[Replace]} SQL function" in {

    val pattern = "\\."
    val replacement = ","
    val expected = regexp_replace(firstCol, pattern, replacement)
    val actual = SqlFunctionParser.parse(s"replace($firstColStr, '$pattern', '$replacement')")
    assertEqualColumns(expected, actual)
  }

  // Substring
  it should s"correctly parse ${className[Substring]} SQL function" in {

    val (start, length) = (0, 2)
    val expected = substring(firstCol, start, length)
    val actual = SqlFunctionParser.parse(f"substring($firstColStr, $start, $length)")
    assertEqualColumns(expected, actual)
  }

  // ToDateOrTimestamp
  it should s"correctly parse ${className[ToDateOrTimestamp]} SQL function" in {

    val format = "yyyy-MM-dd"
    val testSeq: Seq[(String, (Column, String) => Column)] = Seq(
      "Date" -> to_date,
      "Timestamp" -> to_timestamp)

    testSeq foreach {
      case (functionSuffix, expectedFunction) =>
        val actualColumn = SqlFunctionParser.parse(s"to$functionSuffix($firstColStr, '$format')")
        assertEqualColumns(expectedFunction(firstCol, format), actualColumn)
    }
  }

  // Trim
  it should s"correctly parse ${className[Trim]} SQL function" in {

    val expected = trim(firstCol)
    val actual = SqlFunctionParser.parse(s"trim($firstColStr)")
    assertEqualColumns(expected, actual)
  }

  // When
  it should s"correctly parse ${className[When]} SQL function" in {

    val (threshold, label) = (0, "OK")
    val expected = when(firstCol > threshold, label)
    val actual = SqlFunctionParser.parse(s"when(gt($firstColStr, lit($threshold)), lit('$label'))")
    assertEqualColumns(expected, actual)
  }
}
