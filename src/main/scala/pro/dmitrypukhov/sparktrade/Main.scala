package pro.dmitrypukhov.sparktrade

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory
import pro.dmitrypukhov.sparktrade.acquisition.FinamImport
import pro.dmitrypukhov.sparktrade.ingestion.{CandleIngester, TicksIngester}
import pro.dmitrypukhov.sparktrade.lambda.batch.{CandlesProcessor, TicksProcessor}
import pro.dmitrypukhov.sparktrade.lambda.speed.{CandlesStream, TicksStream}
import pro.dmitrypukhov.sparktrade.storage.Lake

import scala.collection.JavaConverters._

/**
 * App entry point
 * Lambda architecture: batch layer for heavy analytics and speed layer for realtime streaming.
 * 1. Acquisition layer reads raw data to lake
 * 2. Messaging layer listens raw streams from price providers
 * 3. Ingestion layer listens messaging layer and transforms raw format to speed layer related
 *
 * 4. Lambda layer contains of Batch and Speed layers
 * 5. Batch layer reads raw data from lake and prepare batch views
 * 6. Speed layer listens ingestion layer and prepare speed views
 *
 * 7. Serving layer is a facade for data marts, calculated by batch and speed layers
 * 8. Datamarts are facades and combine data from speed and batch views
 *
 */
object Main extends App {
  private val log = LoggerFactory.getLogger(this.getClass)
  private val config = ConfigFactory.load()

  private def initSpark(): Unit = {
    // Log parameters from typesafe config
    val configMap = config.entrySet().asScala.map(entry => (entry.getKey, entry.getValue.unwrapped().toString))
    val configLogMsg = configMap.map(t => s"${t._1}=${t._2}").mkString("\n")
    val header = new StringBuilder(
      "\n----------------------------------------------------------\n" +
        "--- Config:\n" +
        "----------------------------------------------------------\n")
      .append(configLogMsg)
    log.info(header.toString)

    // Load spark conf from from scala typesafe config
    val sparkConf = new SparkConf()
    sparkConf.setAll(configMap)

    // Create spark session
    SparkSession.builder().config(
      sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // Initialize data lake
    Lake.init()
  }

  /**
   * Exec Acquisition Layer jobs. Import raw data into Lake.
   */
  def execAquisition() {
    // Import
    new FinamImport().importCandles()
    new FinamImport().importTicks()
  }

  /**
   * Start Ingestion Layer jobs. Start ingestion layer streamings
   */
  def ingest(): Unit = {
    // This command starts streaming raw data into Lake for Batch Layer.
    // Streaming for Speed layer is initialized, but will be started later by Speed Layer.
    new CandleIngester().start()
    new TicksIngester().start().awaitTermination()
  }

  /**
   * Exec Batch Layer jobs. Recalculate batch views for Data Marts.
   */
  def execBatch(): Unit = {
    // Batch processing
    new CandlesProcessor().process()
    new TicksProcessor().process()
  }

  /**
   * Run speed layer streaming
   */
  def runSpeed(): Unit = {
    // Source stream from Ingestion Layer
    val candlesSrc = new CandleIngester().stream
    // Create candles stream of Speed Layer
    val candles = new CandlesStream().process(candlesSrc)
    candles
      .writeStream
      .format("console")
      .start()
      //.awaitTermination(3000)

    // Source stream from Ingestion Layer
    val ticksSrc = new TicksIngester().stream
    // Create ticks stream of Speed Layer
    val ticks = new TicksStream().process(ticksSrc)
    ticks
      .writeStream
      .format("console")
      .start()
      .awaitTermination(5000)

  }

  //////////////////////////////////////////////////////////////////////////////////
  ////////////////////// Main  /////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////

  // Configure and create Spark session
  initSpark()

  // Acquisition Layer. Import raw data
  //execAquisition()

  // Ingestion Layer.
  //ingest()

  // Batch processing
  //execBatch()
  runSpeed()

  // Querying data mart
  //  val candles = new PriceMart().candles("RI.RTSI", java.sql.Date.valueOf("2018-01-30"))
  //  candles.show()

  println("Completed")
}
