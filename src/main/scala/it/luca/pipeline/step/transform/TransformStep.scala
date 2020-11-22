package it.luca.pipeline.step.transform

import it.luca.pipeline.json.DecodeJsonDerive
import it.luca.pipeline.step.common.AbstractStep
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

case class TransformStep(override val name: String,
                         override val description: String,
                         override val stepType: String,
                         override val dataframeId: String)
                         //transformationOptions: TransformationOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  def transform(dataframeMap: mutable.Map[String, DataFrame]): DataFrame = {
    dataframeMap("1")
  }
}

object TransformStep extends DecodeJsonDerive[TransformStep]
