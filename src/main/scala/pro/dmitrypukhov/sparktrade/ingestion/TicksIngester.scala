package pro.dmitrypukhov.sparktrade.ingestion

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.DataStreamWriter
import pro.dmitrypukhov.sparktrade.storage.Lake

/**
 * Ingests ticks data using Spark Streaming
 */
class TicksIngester extends BaseIngester {
  /**
   * Stream to persist csv data into Lake
   */
  override val persistingStream: DataStreamWriter[Row] = createPersistingStream(
    config.getString("sparktrade.ingestion.ticks.load.path"),
    Lake.rawFinamTicksDir,
    "ticks")

  /**
   * Stream to use in Speed layer
   */
  override val stream: DataFrame = createStream(config.getString("sparktrade.ingestion.ticks.load.path"), "ticks")
}
