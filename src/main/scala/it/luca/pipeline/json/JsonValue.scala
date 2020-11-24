package it.luca.pipeline.json

object JsonValue extends Enumeration {

  protected case class Val(value: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val CsvSource: Val = Val("csv")
  val DateType: Val = Val("date")
  val DropColumnTransformation: Val = Val("drop")
  val IntType: Val = Val("int")
  val HiveSource: Val = Val("hive")
  val JDBCSource: Val = Val("jdbc")
  val ReadStep: Val = Val("read")
  val SelectTransformation: Val = Val("select")
  val StringType: Val = Val("string")
  val TimestampType: Val = Val("timestamp")
  val TransformStep: Val = Val("transform")
  val WithColumnTransformation: Val = Val("withColumn")
  val WriteStep: Val = Val("writeStep")
}
