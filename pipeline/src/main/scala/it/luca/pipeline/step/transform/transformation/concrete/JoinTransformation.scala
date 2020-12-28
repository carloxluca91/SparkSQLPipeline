package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.option.concrete.{JoinSelectColumn, JoinTransformationOptions, SingleJoinCondition}
import it.luca.pipeline.step.transform.transformation.common.MultipleSrcTransformation
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

import scala.collection.mutable

object JoinTransformation extends MultipleSrcTransformation[JoinTransformationOptions] {

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
          val leftSideExpressionCol: Column = SQLFunctionParser.parse(singleJoinCondition.leftSide, leftDf)
          val rightSideExpressionCol: Column = SQLFunctionParser.parse(singleJoinCondition.rightSide, rightDf)
          val operator: (Column, Column) => Column = resolveOperator(singleJoinCondition.operator)

          logger.info(s"Successfully parsed ${classOf[SingleJoinCondition].getSimpleName} # $index: ${singleJoinCondition.toString}")
          operator(leftSideExpressionCol, rightSideExpressionCol)
        }).reduce(_ && _)
    }

  private val defineSelectColumns: (JoinTransformationOptions, DataFrame, DataFrame) => Seq[Column] =
    (joinTransformationOptions, leftDf, rightDf) => {

      // Define columns to select from involved dataframes
      val numberOfColumnsToSelect = joinTransformationOptions
        .joinOptions
        .selectColumns.length

      joinTransformationOptions.joinOptions
        .selectColumns
        .zip(1 to numberOfColumnsToSelect)
        .map(tuple2 => {

          // Detect the dataframe to be "linked" to current expression, parse the expression adding an alias if defined
          val (joinSelectColumn, index): (JoinSelectColumn, Int) = tuple2
          val (currentSideDfId, currentSideDf): (String, DataFrame) = joinSelectColumn.side.toLowerCase match {
            case "left" => (joinTransformationOptions.inputDfIds.left, leftDf)
            case "right" => (joinTransformationOptions.inputDfIds.right, rightDf)
          }

          val selectedColumn: Column = SQLFunctionParser.parse(joinSelectColumn.expression, currentSideDf)
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

    val inputSourceIds = transformationOptions.inputDfIds
    val joinOptions = transformationOptions.joinOptions
    val leftDataframe: DataFrame = dataframeMap(inputSourceIds.left)
    val rightDataframe: DataFrame = dataframeMap(inputSourceIds.right)

    val joinCondition: Column = assemblyJoinCondition(joinOptions.joinCondition, leftDataframe, rightDataframe)
    val selectColumns: Seq[Column] = defineSelectColumns(transformationOptions, leftDataframe, rightDataframe)

    leftDataframe
      .join(rightDataframe, joinCondition, joinOptions.joinType)
      .select(selectColumns: _*)
  }
}
