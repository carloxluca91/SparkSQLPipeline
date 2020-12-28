package it.luca.spark.sql.catalog.common

import java.text.{CharacterIterator, StringCharacterIterator}

import it.luca.spark.sql.catalog.functions.SQLCatalog
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.collection.mutable.ListBuffer

abstract class UnboundedColumnSQLFunction(override val expression: String,
                                          override val catalogExpression: SQLCatalog.Value,
                                          val subExpressionGroupIndex: Int)
  extends AbstractSQLFunction(expression, catalogExpression) {

  private val log = Logger.getLogger(getClass)

  private def splitIntoSubExpressions(str: String): Seq[String] = {

    val subExpressions: ListBuffer[String] = ListBuffer.empty[String]
    var (numberOfOpenBrackets, startIndex) = (0, 0)
    val iterator = new StringCharacterIterator(str)
    while (iterator.current != CharacterIterator.DONE) {

      // Update number of open brackets according to current char
      val currentChar = iterator.current
      numberOfOpenBrackets = currentChar match {
        case '(' => numberOfOpenBrackets + 1
        case ')' => numberOfOpenBrackets - 1
        case _ => numberOfOpenBrackets
      }

      // If a closing bracket is met and the count's value is 0, that is the end of a sub expression
      if (numberOfOpenBrackets == 0 && currentChar == ')') {

        val expressionToAdd = str.substring(startIndex, iterator.getIndex + 1).trim
        log.info(s"Detected subexpression # ${subExpressions.length + 1}: <$expressionToAdd>")
        subExpressions += expressionToAdd

        // Skip current char (a closing bracket) and next (expected to be a comma)
        startIndex = iterator.getIndex + 2
      }

      iterator.next()
    }
    subExpressions
  }

  log.info(s"Group # $subExpressionGroupIndex: <${group(subExpressionGroupIndex)}>")
  final val subExpressions: Seq[String] = splitIntoSubExpressions(group(subExpressionGroupIndex))
  protected val combiningFunction: Seq[Column] => Column

  override protected def asString: String = s"$functionName(${subExpressions.mkString(", ")})"

  def getColumn(inputColumns: Column*): Column = combiningFunction(inputColumns)
}
