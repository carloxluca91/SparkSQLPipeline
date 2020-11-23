package it.luca.pipeline.json

object JsonField extends Enumeration {

  protected case class Val(label: String) extends super.Val

  import scala.language.implicitConversions
  implicit def valueToJsonFieldVal(x: Value): Val = x.asInstanceOf[Val]

  val DbName: Val = Val("dbName")
  val Header: Val = Val("header")
  val JDBCDriver: Val = Val("jdbcDriver")
  val JDBCPassword: Val = Val("jdbcPassword")
  val JDBCUrl: Val = Val("jdbcUrl")
  val JDBCUser: Val = Val("jdbcUser")
  val JDBCUseSSL: Val = Val("jdbcUseSSL")
  val Path: Val = Val("path")
  val SchemaFile: Val = Val("schemaFile")
  val Separator: Val = Val("separator")
  val SourceType: Val = Val("sourceType")
  val StepType: Val = Val("stepType")
  val TableName: Val = Val("tableName")
  val TransformationType: Val = Val("transformationType")

}
