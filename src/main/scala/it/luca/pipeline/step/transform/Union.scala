package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object Union extends MultipleDfTransformation[UnionOptions] {

  private val log = Logger.getLogger(getClass)

  override def transform(transformationOptions: UnionOptions, dataFrames: DataFrame*): DataFrame = {

    val dataFramesIds: Seq[String] = transformationOptions.unionAliases
    val dataFramesToUnite = dataFramesIds.length
    log.info(s"Identified $dataFramesToUnite dataframe(s) to be united (${dataFramesIds.mkString(", ")})")
    val unitedDf: DataFrame = dataFrames.reduce(_ union _)

    log.info(s"Successfully united all of $dataFramesToUnite dataframe(s) (${dataFramesIds.mkString(", ")})")
    unitedDf
  }
}

case class UnionOptions(override val transformationType: String, override val transformationOrder: Int, unionAliases: List[String])
  extends TransformationOptions(transformationType, transformationOrder)

object UnionOptions {

  implicit def decodeJson: DecodeJson[UnionOptions] = DecodeJson.derive[UnionOptions]
}