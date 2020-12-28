package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.transformation.TwoDfTransformation
import it.luca.pipeline.step.transform.{JoinSelectColumn, JoinTransformationOptions, SingleJoinCondition}
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object Join extends TwoDfTransformation[JoinTransformationOptions] {

  private val log = Logger.getLogger(getClass)

  private val assemblyJoinCondition: (Seq[SingleJoinCondition], DataFrame, DataFrame) => Column =
    (singleJoinConditions, leftDf, rightDf) => {

      // Define overall join condition by reducing single equality conditions
      singleJoinConditions
        .zipWithIndex
        .map(tuple2 => {

          // Parse both sides of equality condition and combine them according to provided operator
          val (singleJoinCondition, index): (SingleJoinCondition, Int) = tuple2
          val leftSide: Column = SQLFunctionParser.parse(singleJoinCondition.leftSide, leftDf)
          val rightSide: Column = SQLFunctionParser.parse(singleJoinCondition.rightSide, rightDf)

          log.info(s"Successfully parsed ${classOf[SingleJoinCondition].getSimpleName} # $index: ${singleJoinCondition.toString}")
          leftSide && rightSide
        }).reduce(_ && _)
    }

  private val defineSelectColumns: (JoinTransformationOptions, DataFrame, DataFrame) => Seq[Column] =
    (joinTransformationOptions, leftDataFrame, rightDataFrame) => {

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
            case "left" => ("left", leftDataFrame)
            case "right" => ("right", rightDataFrame)
          }

          val selectedColumn: Column = SQLFunctionParser.parse(joinSelectColumn.expression, currentSideDf)
          val selectedColumnMaybeAliased: Column = joinSelectColumn.alias match {
            case None => selectedColumn
            case Some(x) => selectedColumn.as(x)
          }

          log.info(s"Successfully parsed ${classOf[JoinSelectColumn].getSimpleName} # $index (${joinSelectColumn.expression}) " +
            s"from dataframe '$currentSideDfId'")
          selectedColumnMaybeAliased
        })
    }

  override def transform(transformationOptions: JoinTransformationOptions, firstDataFrame: DataFrame, secondDataFrame: DataFrame): DataFrame = {

    val joinOptions = transformationOptions.joinOptions

    val joinCondition: Column = assemblyJoinCondition(joinOptions.joinCondition, firstDataFrame, secondDataFrame)
    val selectColumns: Seq[Column] = defineSelectColumns(transformationOptions, secondDataFrame, secondDataFrame)

    firstDataFrame
      .join(secondDataFrame, joinCondition, joinOptions.joinType)
      .select(selectColumns: _*)
  }
}
