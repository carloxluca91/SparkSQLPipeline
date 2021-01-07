package it.luca.pipeline.exception

case class ReadHiveTableOptionsException(msg: String)
  extends Throwable(msg)

object ReadHiveTableOptionsException {

  def apply(): ReadHiveTableOptionsException = {

    val msg = s"Both 'tableName' and 'sqlQuery' field are not defined. What should i do ?"
    ReadHiveTableOptionsException(msg)
  }
}
