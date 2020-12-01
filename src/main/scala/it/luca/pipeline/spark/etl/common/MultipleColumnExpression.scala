package it.luca.pipeline.spark.etl.common

import java.text.{CharacterIterator, StringCharacterIterator}

import it.luca.pipeline.spark.etl.parsing.EtlExpression

import scala.collection.mutable.ListBuffer

abstract class MultipleColumnExpression(override val expression: String,
                                        override val etlExpression: EtlExpression.Value)
  extends AbstractExpression(expression, etlExpression) {

  private final def splitIntoSubExpressions(str: String): Seq[String] = {

    val expressions: ListBuffer[String] = ListBuffer.empty[String]
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
        expressions += str.substring(startIndex, iterator.getIndex + 1).trim
        startIndex = iterator.getIndex + 2
      }

      iterator.next()
    }
    expressions
  }

  final val subExpressions: Seq[String] = splitIntoSubExpressions(group(2))
}
