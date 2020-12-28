package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.step.transform.option.concrete._
import it.luca.pipeline.step.transform.transformation.concrete._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class TransformStep(override val name: String,
                         override val description: String,
                         override val stepType: String,
                         inputAlias: String,
                         outputAlias: String,
                         transformations: List[TransformationOptions])
  extends AbstractStep(name, description, stepType, inputAlias) {

  private val logger = Logger.getLogger(getClass)

  def transform(dataframeMap: mutable.Map[String, DataFrame]): DataFrame = {

    // Transform input dataframe according to matched pattern
    val transformedDataframe: DataFrame = transformations match {

      // Transformations that involve more than one dataframe
      case mto: MultipleDfTransformationOptions[_] => mto match {

        case jto: JoinTransformationOptions => Join.transform(jto, dataframeMap)
        case uto: UnionOptions => UnionTransformation.transform(uto, dataframeMap)
      }

      case sto: SingleDfTransformationOptions =>

        // Transformations that involve a single dataframe
        val inputDataframe: DataFrame = dataframeMap(sto.inputAlias)
        sto match {

          case d: DropColumnTransformationOptions => Drop.transform(d, inputDataframe)
          case f: FilterOptions => Filter.transform(f, inputDataframe)
          case s: SelectOptions => Select.transform(s, inputDataframe)
          case r: WithColumnRenamedOptions => WithColumnRenamedTransformation.transform(r, inputDataframe)
          case w: WithColumnTransformationOptions => WithColumnTransformation.transform(w, inputDataframe)
        }
    }

    logger.info(s"Successfully created transformed dataframe '$inputAlias' during transformStep $name")
    transformedDataframe
  }
}

object TransformStep {

  implicit def decodeJson: DecodeJson[TransformStep] = DecodeJson.derive[TransformStep]
}
