package it.luca.pipeline.step.transform.common

import it.luca.pipeline.step.transform.option.JoinTransformationOptions
import org.apache.spark.sql.DataFrame

import scala.collection.mutable

trait MultipleSrcTransformation[T <: JoinTransformationOptions] {

  def transform(transformationOptions: T, dataframeMap: mutable.Map[String, DataFrame]): DataFrame

}
