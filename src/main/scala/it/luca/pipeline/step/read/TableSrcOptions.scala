package it.luca.pipeline.step.read

sealed abstract class TableSrcOptions(override val sourceType: String,
                                      val dbName: String,
                                      val tableName: String)
  extends SrcOptions(sourceType)

case class HiveTableSrcOptions(override val sourceType: String,
                               override val dbName: String,
                               override val tableName: String)
  extends TableSrcOptions(sourceType, dbName, tableName)

case class JDBCTableSrcOptions(override val sourceType: String,
                               override val dbName: String,
                               override val tableName: String,
                               jdbcUrl: String,
                               jdbcDriver: String,
                               jdbcUser: String,
                               jdbcPassWord: String,
                               jdbcUseSSL: Option[String])
  extends TableSrcOptions(sourceType, dbName, tableName)
