package pro.dmitrypukhov.sparktrade.datamarts.prices

import java.time.LocalDateTime

/**
 * Ohlc entity
 */
case class Candle(assetCode: String, datetime: java.sql.Timestamp, open: Double, high: Double, low: Double, close: Double, vol: Double)
