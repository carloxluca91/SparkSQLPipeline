package it.luca.pipeline.etl.parsing

import it.luca.pipeline.etl.common.{AbstractExpression, SingleColumnExpression, StaticColumnExpression}
import it.luca.pipeline.etl.catalog.{ColExpression, LitExpression}
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

object EtlExpressionParser {

  private final val logger = Logger.getLogger(getClass)

  final def parse(etlExpression: String): Column = {

    val matchingEtlExpressions: EtlExpression.ValueSet = EtlExpression.values
      .filter(v => v.regex.findFirstMatchIn(etlExpression).nonEmpty)

    if (matchingEtlExpressions.nonEmpty) {

      val matchingExpression: EtlExpression.Value = matchingEtlExpressions.head
      val abstractExpression: AbstractExpression = matchingExpression match {
        case EtlExpression.Col => ColExpression(etlExpression)
        case EtlExpression.Lit => LitExpression(etlExpression)
        case _ =>
          //TODO: custom exception
          throw new Exception
      }

      abstractExpression match {
        case expression: SingleColumnExpression =>

          val nestedFunctionExpression: String = expression.nestedFunction
          logger.info(s"Detected a ${classOf[SingleColumnExpression].getSimpleName} expression (${expression.asString}) " +
            s"with following nested function $nestedFunctionExpression. Trying to resolve this latter recursively")
          expression.getColumn(EtlExpressionParser.parse(nestedFunctionExpression))

        case expression: StaticColumnExpression =>

          logger.info(s"Detected a ${classOf[StaticColumnExpression].getSimpleName} expression (${expression.asString})")
          expression.getColumn
      }

    } else {
      //TODO: custom exception
      throw new Exception()
    }
  }
}
