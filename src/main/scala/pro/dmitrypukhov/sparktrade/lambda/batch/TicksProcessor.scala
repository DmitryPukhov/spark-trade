package pro.dmitrypukhov.sparktrade.lambda.batch

import pro.dmitrypukhov.sparktrade.datamarts.prices.Tick
import pro.dmitrypukhov.sparktrade.storage.Lake

/**
 * Calculate ticks from raw data for Ticks mart
 */
class TicksProcessor extends BaseProcessor with Serializable {
  def process(): Unit =
    super.process[Tick](Lake.rawFinamTicksDir + "/*.csv", Lake.ticksTableName, converter.asTick, "ticks")

}
