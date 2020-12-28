package it.luca.pipeline.step.transform.transformation.concrete

import it.luca.pipeline.step.transform.option.concrete.UnionTransformationOptions
import it.luca.pipeline.step.transform.transformation.common.MultipleSrcTransformation
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

object UnionTransformation extends MultipleSrcTransformation[UnionTransformationOptions] {

  private val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: UnionTransformationOptions, dataframeMap: mutable.Map[String, DataFrame]): DataFrame = {

    val dataFramesIds: Seq[String] = transformationOptions.inputDfIds
    val dataFramesToUnite = dataFramesIds.length
    logger.info(s"Identified $dataFramesToUnite dataframe(s) to be united (${dataFramesIds.mkString(", ")})")
    val unitedDf: DataFrame = dataFramesIds
      .map(dataframeMap)
      .reduce(_ union _)

    logger.info(s"Successfully united all of $dataFramesToUnite dataframe(s) (${dataFramesIds.mkString(", ")})")
    unitedDf
  }
}
