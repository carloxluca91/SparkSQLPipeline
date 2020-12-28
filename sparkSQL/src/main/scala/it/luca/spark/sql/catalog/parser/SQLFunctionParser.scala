package it.luca.spark.sql.catalog.parser

import it.luca.spark.sql.catalog.common._
import it.luca.spark.sql.catalog.exceptions.{UndefinedSQLFunctionException, UnmatchedSQLCatalogCaseException}
import it.luca.spark.sql.catalog.functions._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object SQLFunctionParser {

  private val log = Logger.getLogger(getClass)

  private def parse(sqlFunction: String, dataframeOpt: Option[DataFrame]): Column = {

    // Detect the matching expressions (hopefully only one)
    val matchingEtlExpressions: SQLCatalog.ValueSet = SQLCatalog.values
      .filter(v => v.regex.findFirstMatchIn(sqlFunction).nonEmpty)

    // If any, match it to its SQL function
    if (matchingEtlExpressions.nonEmpty) {

      val matchingFunction: SQLCatalog.Value = matchingEtlExpressions.head
      val abstractSQLFunction: AbstractSQLFunction = matchingFunction match {

        case SQLCatalog.Alias => Alias(sqlFunction)
        case SQLCatalog.Case => Case(sqlFunction)
        case SQLCatalog.Cast => Cast(sqlFunction)
        case SQLCatalog.Col => Col(sqlFunction)
        case SQLCatalog.Compare => Compare(sqlFunction)
        case SQLCatalog.Concat => Concat(sqlFunction)
        case SQLCatalog.ConcatWs => ConcatWs(sqlFunction)
        case SQLCatalog.CurrentDateOrTimestamp => CurrentDateOrTimestamp(sqlFunction)
        case SQLCatalog.DateFormat => DateFormat(sqlFunction)
        case SQLCatalog.IsNullOrIsNotNull => IsNullOrNotNull(sqlFunction)
        case SQLCatalog.LeftOrRightPad => LeftOrRightPad(sqlFunction)
        case SQLCatalog.LowerOrUpper => LowerOrUpper(sqlFunction)
        case SQLCatalog.Lit => Lit(sqlFunction)
        case SQLCatalog.OrElse => OrElse(sqlFunction)
        case SQLCatalog.Replace => Replace(sqlFunction)
        case SQLCatalog.Substring => Substring(sqlFunction)
        case SQLCatalog.ToDateOrTimestamp => ToDateOrTimestamp(sqlFunction)
        case SQLCatalog.Trim => Trim(sqlFunction)
        case SQLCatalog.When => When(sqlFunction)
        case _ => throw UnmatchedSQLCatalogCaseException(sqlFunction, matchingFunction)
      }

      // Detect pattern of matched catalog expression
      abstractSQLFunction match {
        case singleColumnSQLFunction: SingleColumnSQLFunction =>

          val nestedFunctionExpression: String = singleColumnSQLFunction.nestedFunction
          log.info(s"Detected a ${classOf[SingleColumnSQLFunction].getSimpleName} expression <${singleColumnSQLFunction.toString}> " +
            s"with following nested function <$nestedFunctionExpression>. Trying to resolve this latter recursively")
          singleColumnSQLFunction.getColumn(parse(nestedFunctionExpression, dataframeOpt))

        case staticColumnSQLFunction: StaticColumnSQLFunction =>

          // If matched catalog expression is a Col, check whether dataframeOpt is defined or not in order to associate output column to it
          log.info(s"Detected a ${classOf[StaticColumnSQLFunction].getSimpleName} expression <${staticColumnSQLFunction.toString}>")
          staticColumnSQLFunction match {
            case c: Col => if (dataframeOpt.isEmpty) c.getColumn else dataframeOpt.get(c.columnName)
            case _ => staticColumnSQLFunction.getColumn
          }

        case twoColumnSQLFunction: TwoColumnSQLFunction =>

          log.info(s"Detected a ${classOf[TwoColumnSQLFunction].getSimpleName} expression <${twoColumnSQLFunction.toString}>. " +
            s"Trying to resolve each of the two subexpressions <${twoColumnSQLFunction.firstExpression}, ${twoColumnSQLFunction.secondExpression}> " +
            s"recursively")

          val firstColumn: Column = parse(twoColumnSQLFunction.firstExpression, dataframeOpt)
          val secondColumn: Column = parse(twoColumnSQLFunction.secondExpression, dataframeOpt)
          log.info(s"Successfully parsed both sub expressions")
          twoColumnSQLFunction.getColumn(firstColumn, secondColumn)

        case unboundedColumnSQLFunction: UnboundedColumnSQLFunction =>

          val subExpressions: Seq[String] = unboundedColumnSQLFunction.subExpressions
          log.info(s"Detected a ${classOf[UnboundedColumnSQLFunction].getSimpleName} expression <${unboundedColumnSQLFunction.toString}> " +
            s"with ${subExpressions.size} subexpressions. Trying to resolve each of these recursively")
          val subExpressionColumns: Seq[Column] = subExpressions
            .map(parse(_, dataframeOpt))

          log.info(s"Successfully parsed all of ${subExpressions.size} subexpressions")
          unboundedColumnSQLFunction.getColumn(subExpressionColumns: _*)
      }
    } else {

      // Try to determine "closest" expression
      val functionNameRegex = "^(\\w+)\\(".r
      val exceptionMsgSuffix: String = functionNameRegex.findFirstIn(sqlFunction) match {
        case None => "Unable to provide any hint about function name"
        case Some(x) => s"Hint: provided function name is <$x>. Check the syntax of closest regex within ${SQLCatalog.getClass.getName}"
      }

      val exceptionMsg = s"Unable to match such SQL function <$sqlFunction>. $exceptionMsgSuffix"
      throw UndefinedSQLFunctionException(exceptionMsg)
    }
  }

  final def parse(sqlFunction: String, dataFrame: DataFrame): Column = parse(sqlFunction, Some(dataFrame))

  final def parse(sqlFunction: String): Column = parse(sqlFunction, None)
}
