package it.luca.pipeline.json

object JsonValue extends Enumeration {

  protected case class Val(value: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val CsvSourceOrDestination: Val = Val("csv")
  val DateType: Val = Val("date")
  val DropColumnTransformation: Val = Val("drop")
  val IntType: Val = Val("int")
  val HiveSourceOrDestination: Val = Val("hive")
  val JDBCSourceOrDestination: Val = Val("jdbc")
  val JoinTransformation: Val = Val("join")
  val ReadStep: Val = Val("read")
  val SelectTransformation: Val = Val("select")
  val StringType: Val = Val("string")
  val TimestampType: Val = Val("timestamp")
  val TransformStep: Val = Val("transform")
  val UnionTransformation: Val = Val("union")
  val WithColumnRenamedTransformation: Val = Val("withColumnRenamed")
  val WithColumnTransformation: Val = Val("withColumn")
  val WriteStep: Val = Val("writeStep")
}
