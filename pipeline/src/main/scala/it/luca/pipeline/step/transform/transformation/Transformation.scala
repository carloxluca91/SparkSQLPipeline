package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.step.transform.TransformationOptions
import org.apache.spark.sql.DataFrame

trait Transformation[T <: TransformationOptions] {

  def transform(transformationOptions: T, dataFrame: DataFrame): DataFrame

}

trait TwoDfTransformation[T <: TransformationOptions] {

  def transform(transformationOptions: T, firstDataFrame: DataFrame, secondDataFrame: DataFrame): DataFrame
}

trait MultipleDfTransformation[T <: TransformationOptions] {

  def transform(transformationOptions: T, dataFrames: DataFrame*): DataFrame
}
