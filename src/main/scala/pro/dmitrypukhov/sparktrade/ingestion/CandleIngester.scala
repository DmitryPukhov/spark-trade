package pro.dmitrypukhov.sparktrade.ingestion

import org.apache.spark.sql.streaming.DataStreamWriter
import org.apache.spark.sql.{DataFrame, Row}
import pro.dmitrypukhov.sparktrade.storage.Lake

/**
 * Ingest candle data using Spark Streaming
 */
class CandleIngester extends BaseIngester {

  /**
   * Stream to persist csv data into Lake
   */
  override val persistingStream: DataStreamWriter[Row] = createPersistingStream(
    config.getString("sparktrade.ingestion.candles.load.path"),
    Lake.rawFinamCandlesDir,
    "candles")

  /**
   * Stream to use in Speed layer
   */
  override val stream: DataFrame = createStream(config.getString("sparktrade.ingestion.candles.load.path"), "candles")
}
