package com.positioncalculation

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.IntegerType

object LoadTransactions extends PositionCalcInit {

  /**
    *
    * @param TransactionId
    * @param Instrument
    * @param TransactionType
    * @param TransactionQuantity
    */
  case class InputTran(Instrument: String,
                       TransactionType: String,
                       TransactionQuantity: Int
                      )

  /**
    *
    * @param fileName
    * @return
    */
  def readInputTran(fileName: String): DataFrame = {
    import spark.implicits._
    val read_file = spark.read
      .option("multiline", "true")
      .option("inferSchema","true")
      .json( RESOURCE_ROOT + fileName)
      .withColumn("TransactionId", 'TransactionId.cast(IntegerType))
      .withColumn("TransactionQuantity", 'TransactionQuantity.cast(IntegerType))
      .toDF()

    val total_tran = read_file.groupBy("Instrument","TransactionType").sum("TransactionQuantity")
    val df = total_tran.map(row => InputTran (row.get(0).toString, row.get(1).toString, row.get(2).toString.trim.toInt)).toDF()
    df.show()
    df
  }

}
