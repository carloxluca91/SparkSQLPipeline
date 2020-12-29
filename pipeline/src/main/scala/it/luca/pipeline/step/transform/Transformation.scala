package it.luca.pipeline.step.transform

import argonaut.DecodeJson
import it.luca.pipeline.json.{DecodeJsonSubTypes, JsonField, JsonValue}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

sealed trait Transformation {

  private val log = Logger.getLogger(getClass)

  protected def parseSQLColumns[T, R](tSeq: Seq[T], tToR: T => R, tToString: T => String, category: String): Seq[R] = {

    val numberOfTObjects = tSeq.size
    val prettyCategory = category.replace("column", "column(s)")
    log.info(s"Identified $numberOfTObjects $prettyCategory. Trying to parse each of these")
    val rSeq: Seq[R] = tSeq
      .zip(1 to numberOfTObjects)
      .map(tuple2 => {

        val (tObject, index) = tuple2
        val rObject = tToR(tObject)
        log.info(s"Successfully processed element # $index (${tToString(tObject)}")
        rObject
      })

    log.info(s"Successfully parsed all of $numberOfTObjects $prettyCategory")
    rSeq
  }
}

trait SingleDfTransformation[T <: TransformationOptions] extends Transformation {

  def transform(transformationOptions: T, dataFrame: DataFrame): DataFrame

}

trait TwoDfTransformation[T <: TransformationOptions] extends Transformation {

  def transform(transformationOptions: T, firstDataFrame: DataFrame, secondDataFrame: DataFrame): DataFrame
}

trait MultipleDfTransformation[T <: TransformationOptions] extends Transformation {

  def transform(transformationOptions: T, dataFrames: DataFrame*): DataFrame
}

abstract class TransformationOptions(val transformationType: String, val transformationOrder: Int)

object TransformationOptions extends DecodeJsonSubTypes[TransformationOptions] {

  override protected val discriminatorField: String = JsonField.TransformationType.label
  override protected val subclassesEncoders: Map[String, DecodeJson[_ <: TransformationOptions]] = Map(
    JsonValue.Drop -> DropOptions.decodeJson,
    JsonValue.Join -> JoinTransformationOptions.decodeJson,
    JsonValue.Select -> SelectOptions.decodeJson,
    JsonValue.Union -> UnionOptions.decodeJson,
    JsonValue.WithColumn -> WithColumnOptions.decodeJson,
    JsonValue.WithColumnRenamed -> WithColumnRenamedOptions.decodeJson)
}
