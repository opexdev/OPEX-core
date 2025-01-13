package co.nilin.opex.market.ports.postgres.impl

import co.nilin.opex.common.utils.Interval
import co.nilin.opex.common.utils.hours
import co.nilin.opex.common.utils.minutes
import co.nilin.opex.market.core.inout.*
import co.nilin.opex.market.core.spi.MarketQueryHandler
import co.nilin.opex.market.ports.postgres.dao.OrderRepository
import co.nilin.opex.market.ports.postgres.dao.OrderStatusRepository
import co.nilin.opex.market.ports.postgres.dao.TradeRepository
import co.nilin.opex.market.ports.postgres.model.TradeTickerData
import co.nilin.opex.market.ports.postgres.util.RedisCacheHelper
import co.nilin.opex.market.ports.postgres.util.asOrderDTO
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrElse
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.stereotype.Component
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import java.util.*

@Component
class MarketQueryHandlerImpl(
    private val orderRepository: OrderRepository,
    private val tradeRepository: TradeRepository,
    private val orderStatusRepository: OrderStatusRepository,
    private val redisCacheHelper: RedisCacheHelper
) : MarketQueryHandler {

    //TODO merge order and status fetching in query

    override suspend fun getTradeTickerData(interval: Interval): List<PriceChange> {
        return redisCacheHelper.getOrElse("tradeTickerData:${interval.label}", 2.minutes()) {
            tradeRepository.tradeTicker(interval.getLocalDateTime())
                .collectList()
                .awaitFirstOrElse { emptyList() }
                .map { it.asPriceChangeResponse(Date().time, interval.getTime()) }
        }
    }

    override suspend fun getTradeTickerDateBySymbol(symbol: String, interval: Interval): PriceChange? {
        val cacheId = "tradeTickerData:$symbol:${interval.label}"
        return redisCacheHelper.getOrElse(cacheId, 2.minutes()) {
            tradeRepository.tradeTickerBySymbol(symbol, interval.getLocalDateTime())
                .awaitFirstOrNull()
                ?.asPriceChangeResponse(Date().time, interval.getTime())
        }
    }

    override suspend fun openBidOrders(symbol: String, limit: Int): List<OrderBook> {
        return orderRepository.findBySymbolAndDirectionAndStatusSortDescendingByPrice(
            symbol,
            OrderDirection.BID,
            limit,
            listOf(OrderStatus.NEW.code, OrderStatus.PARTIALLY_FILLED.code)
        ).collectList()
            .awaitFirstOrElse { emptyList() }
            .map { OrderBook(it.price, it.quantity) }
    }

    override suspend fun openAskOrders(symbol: String, limit: Int): List<OrderBook> {
        return orderRepository.findBySymbolAndDirectionAndStatusSortAscendingByPrice(
            symbol,
            OrderDirection.ASK,
            limit,
            listOf(OrderStatus.NEW.code, OrderStatus.PARTIALLY_FILLED.code)
        ).collectList()
            .awaitFirstOrElse { emptyList() }
            .map { OrderBook(it.price, it.quantity) }
    }

    // @Cacheable does not support suspended functions. Spring 6.1 required.
    // @Cacheable(cacheNames = ["marketCache"], key = "'lastOrder'")
    override suspend fun lastOrder(symbol: String): Order? {
        return redisCacheHelper.get<Order>("lastOrder") ?: run {
            val order = orderRepository.findLastOrderBySymbol(symbol).awaitFirstOrNull() ?: return@run null
            val status = orderStatusRepository.findMostRecentByOUID(order.ouid).awaitFirstOrNull()
            order.asOrderDTO(status)
        }.also { redisCacheHelper.put("lastOrder", it) }
    }

    //TODO need better query
    override suspend fun recentTrades(symbol: String, limit: Int): List<MarketTrade> {
        val cacheKey = "recentTrades:${symbol.lowercase()}"
        val recentTradesCache = redisCacheHelper.getList<MarketTrade>(cacheKey)
        if (!recentTradesCache.isNullOrEmpty())
            return recentTradesCache.toList()

        return tradeRepository.findBySymbolSortDescendingByCreateDate(symbol, limit)
            .map {
                val takerOrder = orderRepository.findByOuid(it.takerOuid).awaitFirst()
                val makerOrder = orderRepository.findByOuid(it.makerOuid).awaitFirst()
                val isMakerBuyer = makerOrder.direction == OrderDirection.BID
                MarketTrade(
                    it.symbol,
                    it.baseAsset,
                    it.quoteAsset,
                    it.tradeId,
                    it.matchedPrice,
                    it.matchedQuantity,
                    if (isMakerBuyer)
                        makerOrder.quoteQuantity!!
                    else
                        takerOrder.quoteQuantity!!,
                    Date.from(it.createDate.atZone(ZoneId.systemDefault()).toInstant()),
                    true,
                    isMakerBuyer
                )
            }.toList()
            .onEach { redisCacheHelper.putListItem(cacheKey, it) }
            .also { redisCacheHelper.setExpiration(cacheKey, 60.minutes()) }
    }

    override suspend fun lastPrice(symbol: String?): List<PriceTicker> {
        val list = redisCacheHelper.getOrElse("lastPrice", 1.minutes()) {
            if (symbol.isNullOrEmpty())
                tradeRepository.findAllGroupBySymbol().collectList().awaitFirstOrElse { emptyList() }
            else
                tradeRepository.findBySymbolGroupBySymbol(symbol).collectList().awaitFirstOrElse { emptyList() }
        }
        return list.map { PriceTicker(it.symbol, it.matchedPrice.toString()) }
    }

    override suspend fun getBestPriceForSymbols(symbols: List<String>): List<BestPrice> {
        val id = symbols.joinToString { it }
        return redisCacheHelper.getOrElse("bestPricesForSymbols:$id", 1.minutes()) {
            tradeRepository.bestAskAndBidPrice(symbols)
                .collectList()
                .awaitFirstOrElse { emptyList() }
        }
    }

    override suspend fun getCandleInfo(
        symbol: String,
        interval: String,
        startTime: Long?,
        endTime: Long?,
        limit: Int
    ): List<CandleData> {
        val st = if (startTime == null)
            tradeRepository.findFirstByCreateDate().awaitFirstOrNull()?.createDate ?: LocalDateTime.now()
        else
            with(Instant.ofEpochMilli(startTime)) {
                LocalDateTime.ofInstant(this, ZoneId.systemDefault())
            }

        val et = if (endTime == null)
            tradeRepository.findLastByCreateDate().awaitFirstOrNull()?.createDate ?: LocalDateTime.now()
        else
            with(Instant.ofEpochMilli(endTime)) {
                LocalDateTime.ofInstant(this, ZoneId.systemDefault())
            }

        return tradeRepository.candleData(symbol, interval, st, et, limit)
            .collectList()
            .awaitFirstOrElse { emptyList() }
            .map {
                CandleData(
                    it.openTime,
                    it.closeTime,
                    it.open ?: BigDecimal.ZERO,
                    it.close ?: BigDecimal.ZERO,
                    it.high ?: BigDecimal.ZERO,
                    it.low ?: BigDecimal.ZERO,
                    it.volume ?: BigDecimal.ZERO,
                    BigDecimal.ZERO,
                    it.trades,
                    BigDecimal.ZERO,
                    BigDecimal.ZERO
                )
            }
    }

    override suspend fun numberOfActiveUsers(interval: Interval): Long {
        return redisCacheHelper.getOrElse("activeUsers:${interval.label}", 1.hours()) {
            //TODO remove times(10)
            orderRepository.countUsersWhoMadeOrder(interval.getLocalDateTime())
                .singleOrNull()
                ?.times(10)?.approximate() ?: 0L
        }
    }

    override suspend fun numberOfTrades(interval: Interval, pair: String?): Long {
        return if (pair != null)
            redisCacheHelper.getOrElse("tradeCount:$pair:${interval.label}", 1.hours()) {
                tradeRepository.countBySymbolNewerThan(interval.getLocalDateTime(), pair).singleOrNull()?.approximate()
                    ?: 0
            }
        else
            redisCacheHelper.getOrElse("tradeCount:${interval.label}", 1.hours()) {
                tradeRepository.countNewerThan(interval.getLocalDateTime()).singleOrNull()?.approximate() ?: 0
            }
    }

    override suspend fun numberOfOrders(interval: Interval, pair: String?): Long {
        return if (pair != null)
            redisCacheHelper.getOrElse("orderCount:$pair:${interval.label}", 1.hours()) {
                orderRepository.countBySymbolNewerThan(interval.getLocalDateTime(), pair).singleOrNull()?.approximate()
                    ?: 0
            }
        else
            redisCacheHelper.getOrElse("orderCount:${interval.label}", 1.hours()) {
                orderRepository.countNewerThan(interval.getLocalDateTime()).singleOrNull()?.approximate() ?: 0
            }
    }

    override suspend fun mostIncreasePrice(interval: Interval, limit: Int): List<PriceStat> {
        return redisCacheHelper.getOrElse("priceIncrease:${interval.label}", 1.minutes()) {
            tradeRepository.findByMostIncreasedPrice(interval.getLocalDateTime(), limit)
                .collectList()
                .awaitFirstOrElse { emptyList() }
        }.take(limit)
    }

    override suspend fun mostDecreasePrice(interval: Interval, limit: Int): List<PriceStat> {
        return redisCacheHelper.getOrElse("priceDecrease:${interval.label}", 1.minutes()) {
            tradeRepository.findByMostDecreasedPrice(interval.getLocalDateTime(), limit)
                .collectList()
                .awaitFirstOrElse { emptyList() }
        }.take(limit)
    }

    override suspend fun mostVolume(interval: Interval): TradeVolumeStat? {
        return redisCacheHelper.getOrElse("mostVolume:${interval.label}", 1.minutes()) {
            tradeRepository.findByMostVolume(interval.getLocalDateTime()).awaitSingleOrNull()
        }
    }

    override suspend fun mostTrades(interval: Interval): TradeVolumeStat? {
        return redisCacheHelper.getOrElse("mostTrades:${interval.label}", 1.minutes()) {
            tradeRepository.findByMostTrades(interval.getLocalDateTime()).awaitSingleOrNull()
        }
    }
    override suspend fun getWeeklyPriceData(symbol: String): List<PriceTime> {
        return getPriceDataWithCache(
            symbol = symbol,
            cacheKeyPrefix = "weeklyPriceData",
            interval = "4h",
            fromDate = LocalDateTime.now().minusDays(7)
        )
    }

    override suspend fun getMonthlyPriceData(symbol: String): List<PriceTime> {
        return getPriceDataWithCache(
            symbol = symbol,
            cacheKeyPrefix = "monthlyPriceData",
            interval = "24h",
            fromDate = LocalDateTime.now().minusDays(30)
        )
    }

    override suspend fun getDailyPriceData(symbol: String): List<PriceTime> {
        return getPriceDataWithCache(
            symbol = symbol,
            cacheKeyPrefix = "dailyPriceData",
            interval = "1h",
            fromDate = LocalDateTime.now().minusDays(1)
        )
    }
   private suspend fun getPriceDataWithCache(
        symbol: String,
        cacheKeyPrefix: String,
        interval: String,
        fromDate: LocalDateTime
    ): List<PriceTime> {
        val cacheKey = "${cacheKeyPrefix}:${symbol.lowercase()}"
        val cachedData = redisCacheHelper.getList<PriceTime>(cacheKey)
        if (!cachedData.isNullOrEmpty()) {
            return cachedData.toList()
        }

        return tradeRepository.getPriceTimeData(symbol, interval, fromDate, LocalDateTime.now())
            .collectList()
            .awaitFirstOrElse { emptyList() }
            .let { priceTimes ->
                var lastNonNullPrice: BigDecimal? = null
                val firstNonNullPrice = priceTimes.firstOrNull { it.closePrice != null }?.closePrice ?: BigDecimal.ZERO
                priceTimes.map { item ->
                    val price = item.closePrice ?: lastNonNullPrice ?: firstNonNullPrice
                    lastNonNullPrice = price
                    PriceTime(
                        item.closeTime,
                        price
                    )
                }
                    .onEach { redisCacheHelper.putListItem(cacheKey, it) }
                    .also { redisCacheHelper.setExpiration(cacheKey, 1.hours()) }
            }
    }

    private fun TradeTickerData.asPriceChangeResponse(openTime: Long, closeTime: Long) = PriceChange(
        symbol,
        priceChange ?: BigDecimal.ZERO,
        priceChangePercent ?: BigDecimal.ZERO,
        weightedAvgPrice ?: BigDecimal.ZERO,
        lastPrice ?: BigDecimal.ZERO,
        lastQty ?: BigDecimal.ZERO,
        bidPrice ?: BigDecimal.ZERO,
        askPrice ?: BigDecimal.ZERO,
        openPrice ?: BigDecimal.ZERO,
        highPrice ?: BigDecimal.ZERO,
        lowPrice ?: BigDecimal.ZERO,
        volume ?: BigDecimal.ZERO,
        openTime,
        closeTime,
        firstId ?: -1,
        lastId ?: -1,
        count ?: 0
    )

    private fun Long.approximate(): Long {
        if (this < 10)
            return this

        val str = toString()
        val builder = StringBuilder(str.substring(0, 1))
        repeat(str.length - 1) { builder.append("0") }
        return builder.toString().toLong()
    }
}
