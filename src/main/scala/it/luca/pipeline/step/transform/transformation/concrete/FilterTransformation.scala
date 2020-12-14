package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.option.concrete.FilterTransformationOptions
import it.luca.pipeline.step.transform.transformation.common.SingleSrcTransformation
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object FilterTransformation extends SingleSrcTransformation[FilterTransformationOptions] {

  private val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: FilterTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val filterConditionStr: String = transformationOptions.filterCondition
    val filterConditionColumn: Column = SQLFunctionParser.parse(filterConditionStr)

    logger.info(s"Successfully parsed filter condition $filterConditionStr")
    dataFrame.filter(filterConditionColumn)

  }
}
