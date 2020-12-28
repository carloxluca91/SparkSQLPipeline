package it.luca.pipeline.json

object JsonValue extends Enumeration {

  type JsonValue = String

  val Csv = "csv"
  val Date = "date"
  val Drop = "drop"
  val Int = "int"
  val Hive = "hive"
  //val JDBCSourceOrDestination = "jdbc"
  val Join = "join"
  val Read = "read"
  val Select = "select"
  val String = "string"
  val Timestamp = "timestamp"
  val Transform = "transform"
  val Union = "union"
  val WithColumnRenamed = "withColumnRenamed"
  val WithColumn = "withColumn"
  val Write = "write"
}
