package com.positioncalculation

import org.scalatest.FunSuite

class InternalBuyTest extends FunSuite with SparkSessionTestWrapper {

  test("Read Positions File for transaction type internal buy") {
    val df = Seq(("ABC", "456", "I", 1000, "B", 10))
    val test_df = spark.createDataFrame(df)

    val test_out = CalculatePosition.calcIndividualDeltaPosition(test_df)

    assert(test_out.select("Delta").first().get(0).toString.trim.toInt === -10)
  }

}
