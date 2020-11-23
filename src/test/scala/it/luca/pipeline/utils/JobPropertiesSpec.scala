package it.luca.pipeline.utils

import it.luca.pipeline.test.AbstractSpec

class JobPropertiesSpec extends AbstractSpec {

  private final val existingKey = "jdbc.default.useSSL"
  private final val unExistingKey = "impossible.key.property"
  private final val jobProperties: JobProperties = JobProperties("spark_application.properties")

  s"A ${className[JobProperties]} object" should
    "return the default value when a key is not present" in {

    val defaultValue = "false"
    (existingKey :: unExistingKey :: Nil) foreach {
      key =>
        if (!jobProperties.containsKey(key)) {
          assert(jobProperties.getOrElse(Some(key), defaultValue) == defaultValue)
        } else {
          assert(jobProperties.getOrElse(Some(key), defaultValue) == jobProperties.get(key))
        }
    }
  }

  it should s"return a suitably typed default value when using get(orElse)As[T]" in {

    val defaultValue = false
    (existingKey :: unExistingKey :: Nil) foreach {
      key =>
        if (!jobProperties.containsKey(key)) {
          val returnValue: Boolean = jobProperties.getOrElseAs(Some(key), defaultValue)
          assert(returnValue.isInstanceOf[Boolean])
          assert(returnValue == defaultValue)
        } else {
          val returnValue: Boolean = jobProperties.getAs[Boolean](key)
          assert(returnValue.isInstanceOf[Boolean])
        }
    }
  }
}
