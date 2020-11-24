package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.etl.parsing.EtlExpressionParser
import it.luca.pipeline.step.transform.option.WithColumnTransformationOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.{Column, DataFrame}

object WithColumnTransformation extends SingleSrcTransformation[WithColumnTransformationOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: WithColumnTransformationOptions, dataFrame: DataFrame): DataFrame = {

    val numberOfColumns = transformationOptions.columns.size
    val dataFrameId: String = transformationOptions.inputSourceId
    logger.info(s"Starting to analyze each of the $numberOfColumns column expression(s) to be added to dataframe '$dataFrameId'")
    val columnsToAdd: Seq[(String, Column)] = transformationOptions.columns
      .map(c => {

        val (columnName, etlExpression): (String, String) = (c.alias, c.expression)
        val sparkSQLColumn: Column = EtlExpressionParser.parse(etlExpression)
        if (dataFrame.columns contains columnName.toLowerCase) {
          logger.warn(s"Column '$columnName' is already defined in dataframe '$dataFrameId'. " +
            s"Thus, it will be overridden by this expression: ${sparkSQLColumn.toString()}")
        }

        (columnName, sparkSQLColumn)
      })

    logger.info(s"Successfully analyzed each of the $numberOfColumns column expression(s) to be added to dataframe '$dataFrameId'")

    columnsToAdd
      .foldLeft(dataFrame)((df, tuple2) => {
        df.withColumn(tuple2._1, tuple2._2)
      })
  }
}
