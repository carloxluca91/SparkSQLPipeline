package it.luca.pipeline.spark.etl.parsing

import it.luca.pipeline.exception.{UndefinedCatalogExpression, UnmatchedEtlExpressionException}
import it.luca.pipeline.spark.etl.catalog._
import it.luca.pipeline.spark.etl.common._
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object CatalogParser {

  private val logger = Logger.getLogger(getClass)

  private def parse(catalogExpression: String, dataframeOpt: Option[DataFrame]): Column = {

    // Detect the matching expressions (hopefully only one)
    val matchingEtlExpressions: Catalog.ValueSet = Catalog.values
      .filter(v => v.regex.findFirstMatchIn(catalogExpression).nonEmpty)

    // If any, match it to its catalog counterpart
    if (matchingEtlExpressions.nonEmpty) {

      val matchingExpression = matchingEtlExpressions.head
      val abstractExpression: AbstractExpression = matchingExpression match {
        case Catalog.Alias => Alias(catalogExpression)
        case Catalog.Case => Case(catalogExpression)
        case Catalog.Cast => Cast(catalogExpression)
        case Catalog.Col => Col(catalogExpression)
        case Catalog.Compare => Compare(catalogExpression)
        case Catalog.Concat => Concat(catalogExpression)
        case Catalog.ConcatWs => ConcatWs(catalogExpression)
        case Catalog.CurrentDateOrTimestamp => CurrentDateOrTimestamp(catalogExpression)
        case Catalog.IsNullOrIsNotNull => IsNullOrIsNotNull(catalogExpression)
        case Catalog.Lit => Lit(catalogExpression)
        case Catalog.ToDateOrTimestamp => ToDateOrTimestamp(catalogExpression)
        case Catalog.When => When(catalogExpression)
        case _ => throw UndefinedCatalogExpression(matchingExpression)
      }

      // Detect pattern of matched catalog expression
      abstractExpression match {
        case expression: SingleColumnExpression =>

          val nestedFunctionExpression: String = expression.nestedFunction
          logger.info(s"Detected a ${classOf[SingleColumnExpression].getSimpleName} expression <${expression.toString}> " +
            s"with following nested function <$nestedFunctionExpression>. Trying to resolve this latter recursively")
          expression.getColumn(parse(nestedFunctionExpression, dataframeOpt))

        case staticColumnExpression: StaticColumnExpression =>

          // If matched catalog expression is a Col, check whether dataframeOpt is defined or not in order to associate output column to it
          logger.info(s"Detected a ${classOf[StaticColumnExpression].getSimpleName} expression <${staticColumnExpression.toString}>")
          staticColumnExpression match {
            case c: Col => if (dataframeOpt.isEmpty) c.getColumn else dataframeOpt.get(c.columnName)
            case _ => staticColumnExpression.getColumn
          }

        case twoColumnExpression: TwoColumnExpression =>

          logger.info(s"Detected a ${classOf[TwoColumnExpression].getSimpleName} expression <${twoColumnExpression.toString}>. " +
            s"Trying to resolve each of the two subexpressions <${twoColumnExpression.firstExpression}, ${twoColumnExpression.secondExpression}> " +
            s"recursively")

          val firstColumn: Column = parse(twoColumnExpression.firstExpression, dataframeOpt)
          val secondColumn: Column = parse(twoColumnExpression.secondExpression, dataframeOpt)
          logger.info(s"Successfully parsed both sub expressions")
          twoColumnExpression.getColumn(firstColumn, secondColumn)

        case unboundedColumnExpression: UnboundedColumnExpression =>

          val subExpressions: Seq[String] = unboundedColumnExpression.subExpressions
          logger.info(s"Detected a ${classOf[UnboundedColumnExpression].getSimpleName} expression <${unboundedColumnExpression.toString}> " +
            s"with ${subExpressions.size} subexpressions. Trying to resolve each of these recursively")
          val subExpressionColumns: Seq[Column] = subExpressions
            .map(parse(_, dataframeOpt))

          logger.info(s"Successfully parsed all of ${subExpressions.size} subexpressions")
          unboundedColumnExpression.getColumn(subExpressionColumns: _*)
      }
    } else throw UnmatchedEtlExpressionException(catalogExpression)
  }

  final def parse(catalogExpression: String, dataFrame: DataFrame): Column = parse(catalogExpression, Some(dataFrame))

  final def parse(catalogExpression: String): Column = parse(catalogExpression, None)
}
