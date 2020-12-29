package it.luca.spark.sql.types

import it.luca.spark.sql.test.AbstractSpec
import org.apache.spark.sql.types.{DataType, DataTypes}

class DataTypeUtilsSpec extends AbstractSpec {

  s"A ${DataTypeUtils.getClass.getSimpleName} object" should
    s"return a suitable datatype depending on provided dataType string representation" in {

    val testSeq: Seq[(String, DataType)] = ("xyz" -> DataTypes.StringType) ::
      ("string" -> DataTypes.StringType) ::
      ("int" -> DataTypes.IntegerType) ::
      ("double" -> DataTypes.DoubleType) ::
      ("long" -> DataTypes.LongType) ::
      ("date" -> DataTypes.DateType) ::
      ("timestamp" -> DataTypes.TimestampType) :: Nil

    testSeq foreach {
      case (str, dataType) => assertResult(DataTypeUtils.dataType(str))(dataType)
    }
  }
}
