package pro.dmitrypukhov.sparktrade.lambda.batch

import pro.dmitrypukhov.sparktrade.datamarts.prices.Candle
import pro.dmitrypukhov.sparktrade.storage.Lake

/**
 * Calculate candles from raw data for Candle mart
 */
class CandlesProcessor extends BaseProcessor with Serializable {
  /**
   * Read raw data from lake, transform to Candle entity, save to candles table.
   */
  def process(): Unit = super.process[Candle](Lake.rawFinamCandlesDir + "/*.csv", Lake.candlesTableName, converter.asCandle, "candles")
}
