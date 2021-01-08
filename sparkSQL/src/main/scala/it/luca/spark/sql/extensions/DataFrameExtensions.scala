package it.luca.spark.sql.extensions

import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame

class DataFrameExtensions(private val dataFrame: DataFrame) {

  private val log = Logger.getLogger(getClass)

  def coalesce(numPartitionsOpt: Option[Int]): DataFrame = {

    val (coalesceInfo, dataFrameMaybeCoalesced): (String, DataFrame) = numPartitionsOpt match {
      case None => ("Number of partitions not specified", dataFrame)
      case Some(x) => (s"Decreasing the number of partitions to $x (using .coalesce", dataFrame.coalesce(x))
    }

    log.info(s"$coalesceInfo")
    dataFrameMaybeCoalesced
  }

  def prettySchema: String = s"\n\n${dataFrame.schema.treeString}"

  def saveAsTableOrInsertInto(fqTableName: String, saveMode: String, partitionByOpt: Option[Seq[String]], tablePathOpt: Option[String]): Unit = {

    val dataFrameWriter = dataFrame
      .write
      .mode(saveMode)

    val sparkSession = dataFrame.sparkSession
    if (sparkSession.catalog.tableExists(fqTableName)) {

      // If provided table exists, just .insertInto (and let Hive handle partitioning)
      log.info(s"Hive table '$fqTableName' already exists. So, starting to insert data within it (using .insertInto) using saveMode '$saveMode'")

      dataFrameWriter
        .insertInto(fqTableName)

      log.info(s"Successfully inserted data within Hive table '$fqTableName'")

    } else {

      // Otherwise, .saveAsTable according to provided (or not) partitioning column(s) and HDFS path
      log.warn(s"Hive table '$fqTableName' does not exist. So, creating it now (using .saveAsTable) using saveMode '$saveMode'")

      dataFrameWriter
        .partitionBy(partitionByOpt)
        .path(tablePathOpt)
        .saveAsTable(fqTableName)

      log.info(s"Successfully created Hive table '$fqTableName'")
    }
  }
}
