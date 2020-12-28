package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.FilterOptions
import it.luca.pipeline.step.transform.transformation.Transformation
import it.luca.spark.sql.catalog.parser.SQLFunctionParser
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object Filter extends Transformation[FilterOptions] {

  private val log = Logger.getLogger(getClass)

  override def transform(transformationOptions: FilterOptions, dataFrame: DataFrame): DataFrame = {

    val filterConditionStr: String = transformationOptions.filterCondition
    val filterConditionColumn: Column = SQLFunctionParser.parse(filterConditionStr)

    log.info(s"Successfully parsed filter condition $filterConditionStr")
    dataFrame.filter(filterConditionColumn)

  }
}
