package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.spark.etl.parsing.EtlExpressionParser
import it.luca.pipeline.step.transform.common.SingleSrcTransformation
import it.luca.pipeline.step.transform.option.WithColumnTransformationOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object WithColumnTransformation extends SingleSrcTransformation[WithColumnTransformationOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: WithColumnTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val numberOfColumns = transformationOptions.columns.size
    val dataframeId: String = transformationOptions.inputSourceId
    logger.info(s"Identified $numberOfColumns column(s) to add to dataframe '$dataframeId'. Trying to parse each of these")
    val columnsToAdd: Seq[(String, Column)] = transformationOptions.columns
      .map(c => {

        val (columnName, etlExpression): (String, String) = (c.alias, c.expression)
        val sparkSQLColumn: Column = EtlExpressionParser.parse(etlExpression)
        if (dataFrame.columns contains columnName.toLowerCase) {
          logger.warn(s"Column '$columnName' is already defined on dataframe '$dataframeId'. " +
            s"Thus, it will be overridden by this expression: ${sparkSQLColumn.toString()}")
        }

        (columnName, sparkSQLColumn)
      })

    logger.info(s"Successfully parsed each of the $numberOfColumns column expression(s) to be added to dataframe '$dataframeId'")

    columnsToAdd
      .foldLeft(dataFrame)((df, tuple2) => {
        df.withColumn(tuple2._1, tuple2._2)
      })
  }
}
