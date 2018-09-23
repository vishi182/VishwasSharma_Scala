package com.positioncalculation

import org.scalatest.FunSuite

class ExternalSellTest extends FunSuite with SparkSessionTestWrapper {

  test("Read Positions File for transaction type external sell") {
    val df = Seq(("XYZ", "123", "E", 1000, "S", 10))
    val test_df = spark.createDataFrame(df)

    val test_out = CalculatePosition.calcIndividualDeltaPosition(test_df)

    assert(test_out.select("Delta").first().get(0).toString.trim.toInt === -10)
  }
}
