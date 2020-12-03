package it.luca.pipeline.spark.etl.common

import java.text.{CharacterIterator, StringCharacterIterator}

import it.luca.pipeline.spark.etl.catalog.EtlExpression
import org.apache.log4j.Logger
import org.apache.spark.sql.Column

import scala.collection.mutable.ListBuffer

abstract class UnboundedColumnExpression(override val expression: String,
                                         override val etlExpression: EtlExpression.Value,
                                         val subExpressionGroupIndex: Int)
  extends AbstractExpression(expression, etlExpression) {

  private val logger = Logger.getLogger(getClass)

  private def splitIntoSubExpressions(str: String): Seq[String] = {

    val subExpressions: ListBuffer[String] = ListBuffer.empty[String]
    var (numberOfOpenParentheses, startIndex) = (0, 0)
    val iterator = new StringCharacterIterator(str)
    while (iterator.current != CharacterIterator.DONE) {

      val currentChar = iterator.current
      numberOfOpenParentheses = currentChar match {
        case '(' => numberOfOpenParentheses + 1
        case ')' => numberOfOpenParentheses - 1
        case _ => numberOfOpenParentheses
      }

      if (numberOfOpenParentheses == 0 && currentChar == ')') {

        val expressionToAdd = str.substring(startIndex, iterator.getIndex + 1).trim
        logger.info(s"Detected subexpression # ${subExpressions.length + 1}: <$expressionToAdd>")
        subExpressions += expressionToAdd
        startIndex = iterator.getIndex + 2
      }

      iterator.next()
    }
    subExpressions
  }

  final val subExpressions: Seq[String] = splitIntoSubExpressions(group(subExpressionGroupIndex))
  protected val combiningFunction: Seq[Column] => Column

  override protected def asString: String = s"$functionName(${subExpressions.mkString(", ")})"

  def getColumn(inputColumns: Column*): Column = combiningFunction(inputColumns)
}
