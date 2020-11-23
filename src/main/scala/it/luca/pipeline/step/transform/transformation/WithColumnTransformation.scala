package it.luca.pipeline.step.transform.transformation

import it.luca.pipeline.step.transform.option.WithColumnTransformationOptions
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

object WithColumnTransformation extends AbstractSingleSrcTransformation[WithColumnTransformationOptions] {

  private final val logger = Logger.getLogger(getClass)

  override def transform(transformationOptions: WithColumnTransformationOptions, dataFrame: DataFrame): DataFrame = {

    dataFrame
  }
}
