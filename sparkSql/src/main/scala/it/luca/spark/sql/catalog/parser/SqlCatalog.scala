package it.luca.spark.sql.catalog.parser

object SqlCatalog extends Enumeration {

  protected case class Val(regex: scala.util.matching.Regex) extends super.Val

  implicit def valueToVal(x: Value): Val = x.asInstanceOf[Val]

  val Alias: Val = Val("^(alias)\\((\\w+\\(.*\\)), '(\\w+)'\\)$".r)
  val Case: Val = Val("^(case)\\((.+\\))\\)\\.otherWise\\((\\w+\\(.*\\))\\)$".r)
  val Cast: Val = Val("^(cast)\\((\\w+\\(.*\\)), '(\\w+)'\\)$".r)
  val Col: Val = Val("^(col)\\('(\\w+)'\\)$".r)
  val Compare: Val = Val("^(eq|neq|gt|geq|lt|leq)\\((\\w+\\(.*\\)), (\\w+\\(.*\\))\\)$".r)
  val Concat: Val = Val("^(concat)\\((.+\\))\\)$".r)
  val ConcatWs: Val = Val("^(concatWs)\\('(.+)', (.+)\\)$".r)
  val CurrentDateOrTimestamp: Val = Val("^(currentDate|currentTimestamp)\\(\\)$".r)
  val DateFormat: Val = Val("^(dateFormat)\\((\\w+\\(.*\\)), '(.+)'\\)$".r)
  val IsInOrNotIn: Val = Val("^(isIn|isNotIn)\\((\\w+\\(.*\\)), \\[(.+)]\\)$".r)
  val IsNullOrIsNotNull: Val = Val("^(isNull|isNotNull)\\((\\w+\\(.*\\))\\)$".r)
  val LeftOrRightPad: Val = Val("^([l|r]pad)\\((\\w+\\(.*\\)), (\\d+), '(.+)'\\)$".r)
  val Lit: Val = Val("^(lit)\\(('?.+'?)\\)$".r)
  val LowerOrUpper: Val = Val("^(lower|upper)\\((\\w+\\(.*\\))\\)$".r)
  val OrElse: Val = Val("^(orElse)\\((\\w+\\(.*\\)), (\\w+\\(.*\\))\\)$".r)
  val Replace: Val = Val("^(replace)\\((.+\\)), '(.+)', '(.+)'\\)$".r)
  val Substring: Val = Val("^(substring)\\((\\w+\\(.*\\)), (\\d+), (\\d+)\\)$".r)
  val ToDateOrTimestamp: Val = Val("^(toDate|toTimestamp)\\((\\w+\\(.*\\)), '(.+)'\\)$".r)
  val Trim: Val = Val("^(trim)\\((\\w+\\(.*\\))\\)$".r)
  val When: Val = Val("^(when)\\((\\w+\\(.*\\)), (\\w+\\(.*\\))\\)$".r)

}
