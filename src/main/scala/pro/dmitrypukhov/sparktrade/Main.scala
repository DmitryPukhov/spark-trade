package pro.dmitrypukhov.sparktrade

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import pro.dmitrypukhov.sparktrade.datamarts.prices.PriceMart
import pro.dmitrypukhov.sparktrade.acquisition.FinamImport
import pro.dmitrypukhov.sparktrade.batch.{CandlesProcessor, TicksProcessor}
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

    // Create spark session
    SparkSession.builder().config(
      // Load spark conf from from scala typesafe config
      new SparkConf()
        .setAll(configMap))
      .enableHiveSupport()
      .getOrCreate()

    // Initialize data lake
    Lake.init()
  }

  //////////////////////////////////////////////////////////////////////////////////
  ////////////////////// Main  /////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////
  initSpark()

  //  // Import
  new FinamImport().importCandles()
  new FinamImport().importTicks()

  // Batch processing
  new CandlesProcessor().process()
  new TicksProcessor().process()


  val candles = new PriceMart().candles("RI.RTSI", java.sql.Date.valueOf("2018-01-30"))
  candles.show()

  println("Completed")

}
