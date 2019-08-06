package pro.dmitrypukhov.sparktrade.ingestion

import java.nio.file.Paths
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.streaming.{DataStreamWriter, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

/**
 * Common logic for Price stream: pass to Speed layer and persist to Lake in parallel
 */
trait BaseIngester {
  protected val log: Logger = LoggerFactory.getLogger(this.getClass)
  protected val config: Config = ConfigFactory.load()
  protected val spark: SparkSession = SparkSession.active

  /**
   * Stream writer to persist csv data into Lake
   */
  val persistingStream: DataStreamWriter[Row]

  /**
   * Stream reader for Speed Layer.
   */
  val stream: DataFrame

  /**
   * Initialization. Create new stream for Speed layer.
   */
  protected def createStream(loadPath: String, tag: String): DataFrame = {
    //val loadPath = config.getString("sparktrade.ingestion.candles.load.path")
    log.info(s"Ingestion layer. Init $tag stream for speed layer. Source: $loadPath")
    spark.readStream
      .option("header", value = true)
      .format("csv")
      .load(loadPath)
  }

  /**
   * Initialization. Create new batch stream for persisting data to Data Lake.
   */
  protected def createPersistingStream(srcDir: String, dstDir: String, tag: String): DataStreamWriter[Row] = {
    val interval = config.getString("sparktrade.ingestion.lake.persist.interval")
    log.info(s"Ingestion layer. Init $tag persisting stream with interval $interval. Source:$srcDir")

    // For each batch in stream save it to Hive table
    spark.readStream
      .option("header", value = true)
      .format("csv")
      .load(srcDir)
      .writeStream
      // Set persist interval from config, say 5 seconds
      .trigger(Trigger.ProcessingTime(interval))
      .foreachBatch((df: DataFrame, n: Long) => persistBatch(df, dstDir, n))
  }

  private def persistBatch(df: DataFrame, dstDir: String, num: Long): Unit = {
    val dstDirWithNum = Paths.get(dstDir, num.toString).toString
    log.trace(s"Persisting batch $num to $dstDirWithNum")
    df.write.mode(SaveMode.Overwrite).format("csv").csv(dstDirWithNum)
  }

  def start(): StreamingQuery = {
    // Speed stream will be started in Speed or Service layer, not here.
    // Persisting stream starts here and persists streaming data to the Lake.
    persistingStream.start()
  }
}
