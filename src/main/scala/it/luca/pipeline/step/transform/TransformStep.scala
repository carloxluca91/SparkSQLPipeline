package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.transform.common.{SingleSrcTransformationOptions, TransformationOptions}
import it.luca.pipeline.step.transform.option._
import it.luca.pipeline.step.transform.transformation._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class TransformStep(override val name: String,
                         override val description: String,
                         override val stepType: String,
                         override val dataframeId: String,
                         transformationOptions: TransformationOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  private val logger = Logger.getLogger(getClass)

  def transform(dataframeMap: mutable.Map[String, DataFrame]): DataFrame = {

    // Transform input dataframe according to matched pattern
    val transformedDataframe: DataFrame = transformationOptions match {

      case jto: JoinTransformationOptions => JoinTransformation.transform(jto, dataframeMap)
      case sto: SingleSrcTransformationOptions =>

        // Transformations that involve a single dataframe
        val inputDataframe: DataFrame = dataframeMap(sto.inputSourceId)
        sto match {

          case d: DropColumnTransformationOptions => DropColumnTransformation.transform(d, inputDataframe)
          case f: FilterTransformationOptions => FilterTransformation.transform(f, inputDataframe)
          case s: SelectTransformationOptions => SelectTransformation.transform(s, inputDataframe)
          case r: WithColumnRenamedTransformationOptions => WithColumnRenamedTransformation.transform(r, inputDataframe)
          case w: WithColumnTransformationOptions => WithColumnTransformation.transform(w, inputDataframe)
        }
    }

    logger.info(s"Successfully created transformed dataframe '$dataframeId' during transformStep $name")
    transformedDataframe
  }
}

object TransformStep {

  implicit def decodeJson: DecodeJson[TransformStep] = DecodeJson.derive[TransformStep]
}
