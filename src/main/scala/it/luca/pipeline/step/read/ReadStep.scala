package it.luca.pipeline.step.read

import it.luca.pipeline.json.DecodeJsonDerive
import it.luca.pipeline.step.common.AbstractStep
import it.luca.pipeline.utils.JobProperties
import org.apache.spark.sql.{DataFrame, SparkSession}

case class ReadStep(override val name: String,
                    override val description: String,
                    override val stepType: String,
                    override val dataframeId: String,
                    srcOptions: SrcOptions)
  extends AbstractStep(name, description, stepType, dataframeId) {

  def read(sparkSession: SparkSession, jobProperties: JobProperties): DataFrame = {

    srcOptions match {
      case csvSrcOptions: CsvSrcOptions => CsvReader.read(csvSrcOptions, sparkSession, jobProperties)
      case hiveTableSrcOptions: HiveTableSrcOptions => sparkSession.emptyDataFrame
      case _ => sparkSession.emptyDataFrame
    }
  }
}

object ReadStep extends DecodeJsonDerive[ReadStep]
