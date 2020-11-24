package it.luca.pipeline.step.transform.transformation

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.transform.option.{DropColumnTransformationOptions, JoinTransformationOptions,
  SelectTransformationOptions, SingleSourceTransformationOptions,
  TransformationOptions, WithColumnTransformationOptions}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class TransformStep(override val name: String,
                         override val description: String,
                         override val stepType: String,
                         override val dataframeId: String,
                         transformationOptions: TransformationOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  def transform(dataframeMap: mutable.Map[String, DataFrame]): DataFrame = {

    transformationOptions match {
      case options: JoinTransformationOptions =>

        val leftDataframe: DataFrame = dataframeMap(options.inputSourceIds.head)
        val rightDataframe: DataFrame = dataframeMap(options.inputSourceIds.last)
        rightDataframe

      case options: SingleSourceTransformationOptions =>

        val inputDataframe: DataFrame = dataframeMap(options.inputSourceId)
        options match {
          case d: DropColumnTransformationOptions => DropColumnTransformation.transform(d, inputDataframe)
          case s: SelectTransformationOptions => dataframeMap("a")
          case w: WithColumnTransformationOptions => WithColumnTransformation.transform(w, inputDataframe)
        }
    }
  }
}

object TransformStep {

  implicit def decodeJson: DecodeJson[TransformStep] = DecodeJson.derive[TransformStep]
}
