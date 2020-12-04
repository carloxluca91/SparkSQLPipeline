package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.spark.etl.parsing.CatalogParser
import it.luca.pipeline.step.transform.common.SingleSrcTransformation
import it.luca.pipeline.step.transform.option.FilterTransformationOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object FilterTransformation extends SingleSrcTransformation[FilterTransformationOptions] {

  private val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: FilterTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val filterConditionStr: String = transformationOptions.filterCondition
    val filterConditionColumn: Column = CatalogParser.parse(filterConditionStr)

    logger.info(s"Successfully parsed filter condition $filterConditionStr")
    dataFrame.filter(filterConditionColumn)

  }
}
