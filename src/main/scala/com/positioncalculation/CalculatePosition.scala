package com.positioncalculation

import org.apache.spark.sql.DataFrame

object CalculatePosition extends PositionCalcInit {

  /**
    *
    * @param Instrument
    * @param Account
    * @param AccountType
    * @param Quantity
    * @param Delta
    */
  case class EODPosition (Instrument: String,
                          Account: String,
                          AccountType: String,
                          Quantity: String,
                          Delta: Int)

  /**
    *
    * @param transaction_type
    * @param account_type
    * @param sod_quantity
    * @param transaction_quantity
    * @return
    */
  def getEODPositions(transaction_type: String,
                      account_type: String,
                      sod_quantity: Int,
                      transaction_quantity: Int): Int = {

    val eod_quantity = transaction_type match {
      case "B" => {
        account_type match {
          case "E" => sod_quantity + transaction_quantity
          case "I" => sod_quantity - transaction_quantity
        }
      }
      case "S" => {
        account_type match {
          case "E" => sod_quantity - transaction_quantity
          case "I" => sod_quantity + transaction_quantity
        }
      }
      case _ => sod_quantity
    }
    eod_quantity
  }

  /**
    *
    * @param input
    * @return
    */
  def calcIndividualDeltaPosition(input: DataFrame) : DataFrame = {
    import spark.implicits._
    val individual_tran = input.map( row => {

      val instrument = row.get( 0 ).toString
      val account = row.get( 1 ).toString
      val acc_type = row.get( 2 ).toString
      val sod_quant = row.get( 3 ).toString.trim.toInt
      val tran_type = {if (row.get( 4 ) == null) {"null"} else row.get( 4 )}.toString
      val tran_quant = { if (row.get( 5 ) == null) {"0"} else row.get( 5 )}.toString.trim.toInt

      val eod_quant = getEODPositions( tran_type, acc_type, sod_quant, tran_quant )

      val delta = eod_quant - sod_quant

      EODPosition( instrument, account, acc_type, row.get( 3 ).toString, delta )
    }
    ).toDF()
    individual_tran.show()
    individual_tran
  }

  /**
    *
    * @param input
    * @return
    */
  def calcBulkDeltaPosition(input: DataFrame) : DataFrame = {
    val bulk_tran = input
      .groupBy("Instrument", "Account", "AccountType", "Quantity")
      .sum("Delta").toDF()

    import spark.implicits._
    val final_position = bulk_tran
      .map(row => {
        val instrument = row.get(0).toString
        val account = row.get(1).toString
        val acc_type = row.get(2).toString
        val quantity = row.get(3).toString.trim.toInt
        val delta = row.get(4).toString.trim.toInt

        val final_quant = quantity + delta

        EODPosition(instrument,account,acc_type,final_quant.toString,delta)
      })
    final_position.show()
    final_position.toDF()
  }

  /**
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {

    val sod_df = LoadPositions.readSODPositions(I_SOD_POS)
    val tran_df = LoadTransactions.readInputTran(I_TRAN)

    val join_pos_tran = sod_df.join(tran_df, Seq("Instrument"),"left_outer").toDF()

    val delta_calc = calcIndividualDeltaPosition(join_pos_tran)

    val bulk_delta_pos = calcBulkDeltaPosition(delta_calc)

    bulk_delta_pos.write.csv("out/EndOfDay_Positions.txt")
  }

}
