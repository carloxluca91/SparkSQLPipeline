package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.pipeline.step.common.AbstractStep
import it.luca.spark.sql.extensions._
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class TransformStep(override val name: String, override val description: String, override val stepType: String,
                         inputAlias: String, outputAlias: String, transformations: List[TransformationOptions])
  extends AbstractStep(name, description, stepType, outputAlias) {

  private val log = Logger.getLogger(getClass)

  def transform(dataframeMap: mutable.Map[String, DataFrame]): DataFrame = {

    // Apply sequential transformations to input dataframe according to matched pattern
    val inputDataFrame = dataframeMap(inputAlias)

    log.info(s"Starting to execute each of the ${transformations.length} transformation(s) on input dataFrame '$inputAlias'")
    val transformedDataFrame: DataFrame = transformations
      .sortBy(_.transformationOrder)
      .zip(1 to transformations.length)
      .foldLeft(inputDataFrame)((df, tuple2) => {

        val (transformationOption, index) = tuple2
        log.info(s"Starting to execute transformation # $index (${transformationOption.transformationType})")
        val stepTransformedDataFrame: DataFrame = transformationOption match {
          case d: DropOptions => Drop.transform(d, df)
          case f: FilterOptions => Filter.transform(f, df)
          case j: JoinTransformationOptions =>

            // Define the two dataFrames involved
            val (leftDataFrame, rightDataFrame): (DataFrame, DataFrame) = (df, dataframeMap(j.joinOptions.rightAlias))
            Join.transform(j, leftDataFrame, rightDataFrame)

          case s: SelectOptions => Select.transform(s, df)
          case u: UnionOptions =>

            // Define dataFrames to be concatenated
            val dataFrames = u.unionAliases.map(dataframeMap)
            Union.transform(u, dataFrames: _*)

          case w: WithColumnOptions => WithColumn.transform(w, df)
          case wr: WithColumnRenamedOptions => WithColumnRenamed.transform(wr, df)
        }

        log.info(s"Successfully executed transformation # $index (${transformationOption.transformationType}). " +
          s"New dataFrame schema : ${stepTransformedDataFrame.prettySchema}")
        stepTransformedDataFrame
      })

    log.info(s"Successfully executed all of ${transformations.length} transformation(s) on input dataFrame '$inputAlias'")
    transformedDataFrame
  }
}

object TransformStep {

  implicit def decodeJson: DecodeJson[TransformStep] = DecodeJson.derive[TransformStep]
}
