package it.luca.pipeline.spark.etl.parsing

import it.luca.pipeline.exception.{UndefinedCatalogExpression, UnmatchedEtlExpressionException}
import it.luca.pipeline.spark.etl.catalog.{Col, CurrentDateOrTimestamp, Lit, ToDateOrTimestamp}
import it.luca.pipeline.spark.etl.common._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object EtlExpressionParser {

  private val logger = Logger.getLogger(getClass)

  @throws[UndefinedCatalogExpression]
  @throws[UnmatchedEtlExpressionException]
  private def parse(etlExpression: String, dataframeOpt: Option[DataFrame]): Column = {

    // Detect the matching expressions (hopefully only one)
    val matchingEtlExpressions: EtlExpression.ValueSet = EtlExpression.values
      .filter(v => v.regex.findFirstMatchIn(etlExpression).nonEmpty)

    // If any, match it to its catalog counterpart
    if (matchingEtlExpressions.nonEmpty) {

      val matchingExpression = matchingEtlExpressions.head
      val abstractExpression: AbstractExpression = matchingExpression match {
        case EtlExpression.Col => Col(etlExpression)
        case EtlExpression.CurrentDateOrTimestamp => CurrentDateOrTimestamp(etlExpression)
        case EtlExpression.Lit => Lit(etlExpression)
        case EtlExpression.ToDateOrTimestamp => ToDateOrTimestamp(etlExpression)
        case _ => throw UndefinedCatalogExpression(matchingExpression)
      }

      // Detect pattern of matched catalog expression
      abstractExpression match {
        case expression: SingleColumnExpression =>

          val nestedFunctionExpression: String = expression.nestedFunction
          logger.info(s"Detected a ${classOf[SingleColumnExpression].getSimpleName} expression (${expression.asString}) " +
            s"with following nested function $nestedFunctionExpression. Trying to resolve this latter recursively")
          expression.getColumn(EtlExpressionParser.parse(nestedFunctionExpression, dataframeOpt))

        case staticColumnExpression: StaticColumnExpression =>

          // If matched catalog expression is a Col, check whether dataframeOpt is defined or not in order to associate output column to it
          logger.info(s"Detected a ${classOf[StaticColumnExpression].getSimpleName} expression (${staticColumnExpression.asString})")
          staticColumnExpression match {
            case c: Col => if (dataframeOpt.isEmpty) c.getColumn else dataframeOpt.get(c.columnName)
            case _ => staticColumnExpression.getColumn
          }

        case multipleColumnExpression: MultipleColumnExpression =>

          val subExpressions: Seq[String] = multipleColumnExpression.subExpressions
          logger.info(s"Detected a ${classOf[MultipleColumnExpression].getSimpleName} expression (${multipleColumnExpression.asString}) " +
            s"with ${subExpressions.size} subexpressions. Trying to resolve each of these recursively")
          val subExpressionColumns: Seq[Column] = subExpressions
            .map(EtlExpressionParser.parse(_, dataframeOpt))

          logger.info(s"Successfully parsed all of ${subExpressions.size} subexpressions")
          multipleColumnExpression match {
            case t: TwoColumnExpression => t.getColumn(subExpressionColumns.head, subExpressionColumns(1))
            case u: UnboundedColumnExpression => u.getColumn(subExpressionColumns: _*)
          }
      }
    } else throw UnmatchedEtlExpressionException(etlExpression)
  }

  final def parse(etlExpression: String, dataFrame: DataFrame): Column = parse(etlExpression, Some(dataFrame))

  final def parse(etlExpression: String): Column = parse(etlExpression, None)
}
