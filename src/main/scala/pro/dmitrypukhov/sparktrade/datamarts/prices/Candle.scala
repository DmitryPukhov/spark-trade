package pro.dmitrypukhov.sparktrade.datamarts.prices

/**
 * Ohlc entity
 */
case class Candle(assetCode: String, datetime: java.sql.Timestamp, open: Double, high: Double, low: Double, close: Double, vol: Double)
