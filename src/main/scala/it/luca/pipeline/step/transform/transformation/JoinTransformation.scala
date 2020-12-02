package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.spark.etl.parsing.EtlExpressionParser
import it.luca.pipeline.step.transform.common.MultipleSrcTransformation
import it.luca.pipeline.step.transform.option.{JoinSelectColumn, JoinTransformationOptions, SingleJoinCondition}
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

object JoinTransformation extends MultipleSrcTransformation[JoinTransformationOptions]{

  private val logger = Logger.getLogger(getClass)
  private val resolveOperator: String => (Column, Column) => Column =
    operator => {
      operator.toLowerCase match {
      case "equalTo" => _ === _
      case "notEqualTo" => _ =!= _
    }
  }

  private val assemblyJoinCondition: (Seq[SingleJoinCondition], DataFrame, DataFrame) => Column =
    (singleJoinConditions, leftDf, rightDf) => {

      // Define overall join condition by reducing single equality conditions
      singleJoinConditions
        .zipWithIndex
        .map(tuple2 => {

          // Parse both sides of equality condition and combine them according to provided operator
          val (singleJoinCondition, index): (SingleJoinCondition, Int) = tuple2
          val leftSideExpressionCol: Column = EtlExpressionParser.parse(singleJoinCondition.leftSide, leftDf)
          val rightSideExpressionCol: Column = EtlExpressionParser.parse(singleJoinCondition.rightSide, rightDf)
          val operator: (Column, Column) => Column = resolveOperator(singleJoinCondition.operator)

          logger.info(s"Successfully parsed ${classOf[SingleJoinCondition].getSimpleName} # $index: ${singleJoinCondition.toString}")
          operator(leftSideExpressionCol, rightSideExpressionCol)
      }).reduce(_ && _)
    }

  private val defineSelectColumns: (JoinTransformationOptions, DataFrame, DataFrame) => Seq[Column] =
    (joinTransformationOptions, leftDf, rightDf) => {

      // Define columns to select from involved dataframes
      joinTransformationOptions.selectColumns
        .zipWithIndex
        .map(tuple2 => {

          // Detect the dataframe to be "linked" to current expression, parse the expression adding an alias if defined
          val (joinSelectColumn, index): (JoinSelectColumn, Int) = tuple2
          val (currentSideDfId, currentSideDf): (String, DataFrame) = joinSelectColumn.side.toLowerCase match {
            case "left" => (joinTransformationOptions.leftDataframe, leftDf)
            case "right" => (joinTransformationOptions.rightDataframe, rightDf)
          }

          val selectedColumn: Column = EtlExpressionParser.parse(joinSelectColumn.expression, currentSideDf)
          val selectedColumnMaybeAliased: Column = joinSelectColumn.alias match {
            case None => selectedColumn
            case Some(x) => selectedColumn.as(x)
          }

          logger.info(s"Successfully parsed ${classOf[JoinSelectColumn].getSimpleName} # $index (${joinSelectColumn.expression}) " +
            s"from dataframe '$currentSideDfId'")
          selectedColumnMaybeAliased
        })
    }

  override def transform(transformationOptions: JoinTransformationOptions, dataframeMap: mutable.Map[String, DataFrame]): DataFrame = {

    val leftDataframe: DataFrame = dataframeMap(transformationOptions.leftDataframe)
    val rightDataframe: DataFrame = dataframeMap(transformationOptions.rightDataframe)

    val joinCondition: Column = assemblyJoinCondition(transformationOptions.joinCondition, leftDataframe, rightDataframe)
    val selectColumns: Seq[Column] = defineSelectColumns(transformationOptions, leftDataframe, rightDataframe)

    leftDataframe
      .join(rightDataframe, joinCondition, transformationOptions.joinType)
      .select(selectColumns: _*)
  }
}
