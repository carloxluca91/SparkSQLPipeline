package it.luca.spark.sql.catalog.parser

import it.luca.spark.sql.catalog.common._
import it.luca.spark.sql.catalog.exceptions.{UndefinedSQLFunctionException, UnmatchedSQLCatalogCaseException}
import it.luca.spark.sql.catalog.functions._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object SqlFunctionParser {

  private val log = Logger.getLogger(getClass)

  private def parse(sqlFunction: String, dataframeOpt: Option[DataFrame]): Column = {

    // Detect the matching expressions (hopefully only one)
    val matchingEtlExpressions: SqlCatalog.ValueSet = SqlCatalog.values
      .filter(v => v.regex.findFirstMatchIn(sqlFunction).nonEmpty)

    // If any, match it to its SQL function
    if (matchingEtlExpressions.nonEmpty) {

      val matchingFunction: SqlCatalog.Value = matchingEtlExpressions.head
      val abstractSQLFunction: AbstractSqlFunction = matchingFunction match {

        case SqlCatalog.Alias => Alias(sqlFunction)
        case SqlCatalog.Case => Case(sqlFunction)
        case SqlCatalog.Cast => Cast(sqlFunction)
        case SqlCatalog.Col => Col(sqlFunction)
        case SqlCatalog.Compare => Compare(sqlFunction)
        case SqlCatalog.Concat => Concat(sqlFunction)
        case SqlCatalog.ConcatWs => ConcatWs(sqlFunction)
        case SqlCatalog.CurrentDateOrTimestamp => CurrentDateOrTimestamp(sqlFunction)
        case SqlCatalog.DateFormat => DateFormat(sqlFunction)
        case SqlCatalog.IsInOrNotIn => IsInOrNotIn(sqlFunction)
        case SqlCatalog.IsNullOrIsNotNull => IsNullOrNotNull(sqlFunction)
        case SqlCatalog.LeftOrRightPad => LeftOrRightPad(sqlFunction)
        case SqlCatalog.LowerOrUpper => LowerOrUpper(sqlFunction)
        case SqlCatalog.Lit => Lit(sqlFunction)
        case SqlCatalog.OrElse => OrElse(sqlFunction)
        case SqlCatalog.Replace => Replace(sqlFunction)
        case SqlCatalog.Substring => Substring(sqlFunction)
        case SqlCatalog.ToDateOrTimestamp => ToDateOrTimestamp(sqlFunction)
        case SqlCatalog.Trim => Trim(sqlFunction)
        case SqlCatalog.When => When(sqlFunction)
        case _ => throw UnmatchedSQLCatalogCaseException(sqlFunction, matchingFunction)
      }

      // Detect pattern of matched catalog expression
      abstractSQLFunction match {
        case singleColumnSQLFunction: SingleColumnSqlFunction =>

          val nestedFunctionExpression: String = singleColumnSQLFunction.nestedFunction
          log.info(s"Detected a ${classOf[SingleColumnSqlFunction].getSimpleName} expression `${singleColumnSQLFunction.toString}` " +
            s"with following nested function `$nestedFunctionExpression`. Trying to resolve this latter recursively")
          singleColumnSQLFunction.getColumn(parse(nestedFunctionExpression, dataframeOpt))

        case staticColumnSQLFunction: StaticColumnSqlFunction =>

          // If matched catalog expression is a Col, check whether dataframeOpt is defined or not in order to associate output column to it
          log.info(s"Detected a ${classOf[StaticColumnSqlFunction].getSimpleName} expression `${staticColumnSQLFunction.toString}`")
          staticColumnSQLFunction match {
            case c: Col => if (dataframeOpt.isEmpty) c.getColumn else dataframeOpt.get(c.columnName)
            case _ => staticColumnSQLFunction.getColumn
          }

        case twoColumnSQLFunction: TwoColumnSqlFunction =>

          log.info(s"Detected a ${classOf[TwoColumnSqlFunction].getSimpleName} expression `${twoColumnSQLFunction.toString}`. " +
            s"Trying to resolve each of the two subexpressions " +
            s"`${twoColumnSQLFunction.firstExpression}`, `${twoColumnSQLFunction.secondExpression}` recursively")

          val firstColumn: Column = parse(twoColumnSQLFunction.firstExpression, dataframeOpt)
          val secondColumn: Column = parse(twoColumnSQLFunction.secondExpression, dataframeOpt)
          log.info(s"Successfully parsed both sub expressions")
          twoColumnSQLFunction.getColumn(firstColumn, secondColumn)

        case unboundedColumnSQLFunction: UnboundedColumnSqlFunction =>

          val subExpressions: Seq[String] = unboundedColumnSQLFunction.subExpressions
          log.info(s"Detected a ${classOf[UnboundedColumnSqlFunction].getSimpleName} expression `${unboundedColumnSQLFunction.toString}` " +
            s"with ${subExpressions.size} subexpressions. Trying to resolve each of these recursively")
          val subExpressionColumns: Seq[Column] = subExpressions
            .map(parse(_, dataframeOpt))

          log.info(s"Successfully parsed all of ${subExpressions.size} subexpressions")
          unboundedColumnSQLFunction.getColumn(subExpressionColumns: _*)
      }
    } else {

      // Try to determine "closest" expression
      val functionNameRegex = "^(\\w+)\\(".r
      val exceptionMsgSuffix: String = functionNameRegex.findFirstMatchIn(sqlFunction) match {
        case None => "Unable to provide any hint about function name"
        case Some(rMatch) => s"Hint: provided function name is '${rMatch.group(1)}'. " +
          s"Check the syntax of closest regex within ${SqlCatalog.getClass.getName}"
      }

      val exceptionMsg = s"Unable to match such Sql function `$sqlFunction`. $exceptionMsgSuffix"
      throw UndefinedSQLFunctionException(exceptionMsg)
    }
  }

  final def parse(sqlFunction: String, dataFrame: DataFrame): Column = parse(sqlFunction, Some(dataFrame))

  final def parse(sqlFunction: String): Column = parse(sqlFunction, None)
}
