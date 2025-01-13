package co.nilin.opex.market.core.spi

import co.nilin.opex.common.utils.Interval
import co.nilin.opex.market.core.inout.*

interface MarketQueryHandler {

    suspend fun getTradeTickerData(interval: Interval): List<PriceChange>

    suspend fun getTradeTickerDateBySymbol(symbol: String, interval: Interval): PriceChange?

    suspend fun openBidOrders(symbol: String, limit: Int): List<OrderBook>

    suspend fun openAskOrders(symbol: String, limit: Int): List<OrderBook>

    suspend fun lastOrder(symbol: String): Order?

    suspend fun recentTrades(symbol: String, limit: Int): List<MarketTrade>

    suspend fun lastPrice(symbol: String?): List<PriceTicker>

    suspend fun getBestPriceForSymbols(symbols: List<String>): List<BestPrice>

    suspend fun getCandleInfo(
        symbol: String,
        interval: String,
        startTime: Long?,
        endTime: Long?,
        limit: Int
    ): List<CandleData>

    suspend fun numberOfActiveUsers(interval: Interval): Long

    suspend fun numberOfTrades(interval: Interval, pair: String? = null): Long

    suspend fun numberOfOrders(interval: Interval, pair: String? = null): Long

    suspend fun mostIncreasePrice(interval: Interval, limit: Int): List<PriceStat>

    suspend fun mostDecreasePrice(interval: Interval, limit: Int): List<PriceStat>

    suspend fun mostVolume(interval: Interval): TradeVolumeStat?

    suspend fun mostTrades(interval: Interval): TradeVolumeStat?

    suspend fun getWeeklyPriceData(symbol: String): List<PriceTime>

    suspend fun getMonthlyPriceData(symbol: String): List<PriceTime>

    suspend fun getDailyPriceData(symbol: String): List<PriceTime>
}