package co.nilin.opex.market.ports.postgres.dao

import co.nilin.opex.market.core.inout.BestPrice
import co.nilin.opex.market.core.inout.PriceStat
import co.nilin.opex.market.core.inout.TradeVolumeStat
import co.nilin.opex.market.core.inout.Transaction
import co.nilin.opex.market.ports.postgres.model.*
import kotlinx.coroutines.flow.Flow
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.query.Param
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.LocalDateTime
import java.util.*

@Repository
interface TradeRepository : ReactiveCrudRepository<TradeModel, Long> {

    @Query("select * from trades where :ouid in (taker_ouid, maker_ouid)")
    fun findByOuid(@Param("ouid") ouid: String): Flow<TradeModel>

    @Query("select * from trades where symbol = :symbol order by create_date desc limit 1")
    fun findMostRecentBySymbol(symbol: String): Flow<TradeModel>

    @Query(
            """
        select * from trades where :uuid in (taker_uuid, maker_uuid) 
            and (:fromTrade is null or id > :fromTrade) 
            and (:symbol is null or symbol = :symbol) 
            and (:startTime is null or trade_date >= :startTime) 
            and (:endTime is null or trade_date < :endTime)
        order by trade_date DESC 
        limit :limit
        """
    )
    fun findByUuidAndSymbolAndTimeBetweenAndTradeIdGreaterThan(
            @Param("uuid")
            uuid: String,
            @Param("symbol")
            symbol: String?,
            @Param("fromTrade")
            fromTrade: Long?,
            @Param("startTime")
            startTime: Date?,
            @Param("endTime")
            endTime: Date?,
            limit: Int
    ): Flow<TradeModel>

    @Query("select * from trades where symbol = :symbol order by create_date desc limit :limit")
    fun findBySymbolSortDescendingByCreateDate(
            @Param("symbol")
            symbol: String,
            @Param("limit")
            limit: Int
    ): Flow<TradeModel>

    @Query(
            """
        with first_trade as (select id, symbol, matched_price, matched_quantity from trades where id in (select min(id) from trades where create_date > :date group by symbol)),
            last_trade as (select id, symbol, matched_price, matched_quantity from trades where id in (select max(id) from trades where create_date > :date group by symbol))
        select symbol, 
        (select matched_price from last_trade where symbol=t.symbol) - (select matched_price from first_trade where symbol=t.symbol) as price_change,
        ((((select matched_price from last_trade where symbol=t.symbol) - (select matched_price from first_trade where symbol=t.symbol))/(select matched_price from first_trade where symbol=t.symbol))*100) as price_change_percent, 
        (sum(matched_quantity)/sum(matched_price)) as weighted_avg_price,
        (select matched_price from last_trade where symbol=t.symbol) as last_price, 
        (select matched_quantity from last_trade where symbol=t.symbol) as last_qty, 
        (
            select price from orders
            inner join open_orders oo on orders.ouid = oo.ouid
            where create_date > :date and symbol=t.symbol and side='BID'
            order by create_date desc limit 1
        ) as bid_price,
        (
            select price from orders
            inner join open_orders oo on orders.ouid = oo.ouid
            where create_date > :date and symbol=t.symbol and side='ASK'
            order by create_date desc limit 1
        ) as ask_price,
        (
            select price from orders
            inner join open_orders oo on orders.ouid = oo.ouid
            where create_date > :date and symbol=t.symbol
            order by create_date desc limit 1
        ) as open_price,
        max(matched_price) as high_price, 
        min(matched_price) as low_price, 
        sum(matched_quantity) as volume, 
        (select id from first_trade where symbol=t.symbol) as first_id, 
        (select id from last_trade where symbol=t.symbol) as last_id, 
        count(id) as count
        from trades as t 
        where create_date > :date
        group by symbol
        """
    )
    fun tradeTicker(@Param("date") createDate: LocalDateTime): Flux<TradeTickerData>

    @Query(
            """
        with first_trade as (select * from trades where create_date > :date and symbol = :symbol order by create_date limit 1),
             last_trade as (select * from trades where create_date > :date and symbol = :symbol order by create_date desc limit 1)
        select symbol, 
        (select matched_price from last_trade) - (select matched_price from first_trade) as price_change,
        ((((select matched_price from last_trade) - (select matched_price from first_trade))/(select matched_price from first_trade))*100) as price_change_percent, 
        (sum(matched_quantity)/sum(matched_price)) as weighted_avg_price,
        (select matched_price from last_trade) as last_price, 
        (select matched_quantity from last_trade) as last_qty, 
        (
            select price from orders
            inner join open_orders oo on orders.ouid = oo.ouid
            where create_date > :date and symbol=t.symbol and side='BID'
            order by create_date desc limit 1
        ) as bid_price,
        (
            select price from orders
            inner join open_orders oo on orders.ouid = oo.ouid
            where create_date > :date and symbol=t.symbol and side='ASK'
            order by create_date desc limit 1
        ) as ask_price,
        (
            select price from orders
            inner join open_orders oo on orders.ouid = oo.ouid
            where create_date > :date and symbol=t.symbol
            order by create_date desc limit 1
        ) as open_price,
        max(matched_price) as high_price, 
        min(matched_price) as low_price, 
        sum(matched_quantity) as volume, 
        (select id from first_trade) as first_id, 
        (select id from last_trade) as last_id, 
        count(id) as count
        from trades as t 
        where create_date > :date and symbol = :symbol
        group by symbol
        """
    )
    fun tradeTickerBySymbol(
            @Param("symbol")
            symbol: String,
            @Param("date")
            createDate: LocalDateTime,
    ): Mono<TradeTickerData>

    @Query(
            """
            select symbol, 
            (
                select price from orders
                inner join open_orders oo on orders.ouid = oo.ouid
                where symbol = t.symbol and side='BID'
                order by create_date desc limit 1
            ) as bid_price,
            (
                select price from orders
                inner join open_orders oo on orders.ouid = oo.ouid
                where symbol = t.symbol and side='ASK'
                order by create_date limit 1
            ) as ask_price
            from trades as t
            group by symbol
        """
    )
    fun bestAskAndBidPrice(): Flux<BestPrice>

    @Query(
            """
            select symbol,
            (
                select price from orders
                inner join open_orders oo on orders.ouid = oo.ouid
                where symbol = t.symbol and side='BID'
                order by create_date desc limit 1
            ) as bid_price,
            (
                select price from orders
                inner join open_orders oo on orders.ouid = oo.ouid
                where symbol = t.symbol and side='ASK'
                order by create_date limit 1
            ) as ask_price
            from trades as t 
            where symbol in (:symbols)
            group by symbol
        """
    )
    fun bestAskAndBidPrice(symbols: List<String>): Flux<BestPrice>

    @Query(
            """
            select symbol, 
            (
                select price from orders
                inner join open_orders oo on orders.ouid = oo.ouid
                where symbol = t.symbol and side='BID'
                order by create_date desc limit 1
            ) as bid_price,
            (
                select price from orders
                inner join open_orders oo on orders.ouid = oo.ouid
                where symbol = t.symbol and side='ASK'
                order by create_date limit 1
            ) as ask_price
            from trades as t 
            where symbol = :symbol
            group by symbol
        """
    )
    fun bestAskAndBidPrice(symbol: String): Mono<BestPrice>

    @Query("select symbol, matched_price from trades where create_date in (select max(create_date) from trades group by symbol) and symbol = :symbol")
    fun findBySymbolGroupBySymbol(@Param("symbol") symbol: String): Flux<LastPrice>

    @Query("select symbol, matched_price from trades where create_date in (select max(create_date) from trades group by symbol)")
    fun findAllGroupBySymbol(): Flux<LastPrice>

    @Query(
            """
        WITH intervals AS (SELECT * FROM interval_generator((TO_TIMESTAMP(:startTime)) ::TIMESTAMP WITHOUT TIME ZONE, (:endTime), :interval ::INTERVAL)), 
        first_trade AS (
            SELECT DISTINCT ON (f.start_time) f.start_time, f.end_time, t.matched_price AS open_price FROM intervals f 
            LEFT JOIN trades t ON t.create_date >= f.start_time AND t.create_date < f.end_time AND t.symbol = :symbol
            ORDER BY f.start_time, t.create_date
        ), last_trade AS (
            SELECT DISTINCT ON (f.start_time) f.start_time,  f.end_time,  t.matched_price AS close_price FROM intervals f
            LEFT JOIN trades t ON t.create_date >= f.start_time AND t.create_date < f.end_time AND t.symbol = :symbol
            ORDER BY f.start_time, t.create_date DESC
        )
        SELECT 
            i.start_time AS open_time,
            i.end_time AS close_time, 
            ft.open_price AS open,
            MAX(t.matched_price) AS high,
            MIN(t.matched_price) AS low,
            lt.close_price AS close,
            SUM(t.matched_quantity) AS volume,
            COUNT(t.id) AS trades
        FROM intervals i
        LEFT JOIN trades t
        ON t.create_date >= i.start_time AND t.create_date < i.end_time AND t.symbol = :symbol
        LEFT JOIN first_trade ft
        ON i.start_time = ft.start_time
        LEFT JOIN last_trade lt
        ON i.start_time = lt.start_time
        GROUP BY i.start_time, i.end_time, ft.open_price, lt.close_price
        ORDER BY i.start_time;
        """
    )
    suspend fun candleData(
            @Param("symbol")
            symbol: String,
            @Param("interval")
            interval: String,
            @Param("startTime")
            startTime: LocalDateTime,
            @Param("endTime")
            endTime: LocalDateTime,
            @Param("limit")
            limit: Int,
    ): Flux<CandleInfoData>

    @Query("select * from trades order by create_date desc limit 1")
    suspend fun findLastByCreateDate(): Mono<TradeModel>

    @Query("select * from trades order by create_date limit 1")
    suspend fun findFirstByCreateDate(): Mono<TradeModel>

    @Query("select count(*) from trades where create_date >= :interval")
    fun countNewerThan(interval: LocalDateTime): Flow<Long>

    @Query("select count(*) from trades where symbol = :symbol and create_date >= :interval")
    fun countBySymbolNewerThan(interval: LocalDateTime, symbol: String): Flow<Long>

    @Query(
            """
        WITH first_trade AS (SELECT symbol, MIN(id) AS min_id FROM trades WHERE create_date > :since GROUP BY symbol),
        last_trade AS (SELECT symbol, MAX(id) AS max_id FROM trades WHERE create_date > :since GROUP BY symbol),
        first_trade_details AS (SELECT ft.symbol, t.matched_price AS first_price FROM first_trade ft JOIN trades t ON ft.min_id = t.id),
        last_trade_details AS (SELECT lt.symbol, t.matched_price AS last_price FROM last_trade lt JOIN trades t ON lt.max_id = t.id)
        SELECT
            t.symbol,
            COALESCE(ltd.last_price, 0.0) AS last_price,
            COALESCE(ltd.last_price - ftd.first_price, 0.0) AS price_change,
            COALESCE(((ltd.last_price - ftd.first_price) / ftd.first_price) * 100, 0.0) AS price_change_percent
        FROM trades t
        JOIN first_trade_details ftd ON t.symbol = ftd.symbol
        JOIN last_trade_details ltd ON t.symbol = ltd.symbol
        WHERE t.create_date > :since
        GROUP BY t.symbol, ftd.first_price, ltd.last_price
        ORDER BY price_change_percent DESC
        LIMIT :limit;
    """
    )
    fun findByMostIncreasedPrice(since: LocalDateTime, limit: Int): Flux<PriceStat>

    @Query(
            """
        WITH first_trade AS (SELECT symbol, MIN(id) AS min_id FROM trades WHERE create_date > :since GROUP BY symbol),
        last_trade AS (SELECT symbol, MAX(id) AS max_id FROM trades WHERE create_date > :since GROUP BY symbol),
        first_trade_details AS (SELECT ft.symbol, t.matched_price AS first_price FROM first_trade ft JOIN trades t ON ft.min_id = t.id),
        last_trade_details AS (SELECT lt.symbol, t.matched_price AS last_price FROM last_trade lt JOIN trades t ON lt.max_id = t.id)
        SELECT
            t.symbol,
            COALESCE(ltd.last_price, 0.0) AS last_price,
            COALESCE(ltd.last_price - ftd.first_price, 0.0) AS price_change,
            COALESCE(((ltd.last_price - ftd.first_price) / ftd.first_price) * 100, 0.0) AS price_change_percent
        FROM trades t
        JOIN first_trade_details ftd ON t.symbol = ftd.symbol
        JOIN last_trade_details ltd ON t.symbol = ltd.symbol
        WHERE t.create_date > :since
        GROUP BY t.symbol, ftd.first_price, ltd.last_price
        ORDER BY price_change_percent
        LIMIT :limit;
    """
    )
    fun findByMostDecreasedPrice(since: LocalDateTime, limit: Int): Flux<PriceStat>

    @Query(
            """
        with first_trade as (select symbol, matched_quantity mq from trades where id in (select min(id) from trades where create_date > :since group by symbol)),
             last_trade as (select  symbol, matched_quantity mq from trades where id in (select max(id) from trades where create_date > :since group by symbol))
        select 
            symbol, 
            coalesce(sum(matched_quantity), 0.0) as volume, 
            count(id) as trade_count,
            coalesce(
                (
                    (select mq from last_trade where symbol = t.symbol)
                  - (select mq from first_trade where symbol = t.symbol)
                ) / (select mq from first_trade where symbol = t.symbol) * 100,
                0.0
            ) as change
        from trades t
        where create_date > :since
        group by symbol
        order by volume
        limit 1
    """
    )
    fun findByMostVolume(since: LocalDateTime): Mono<TradeVolumeStat>

    @Query(
            """
        with first_trade as (select symbol, matched_quantity mq from trades where id in (select min(id) from trades where create_date > :since group by symbol)),
             last_trade as (select  symbol, matched_quantity mq from trades where id in (select max(id) from trades where create_date > :since group by symbol))
        select 
            symbol, 
            coalesce(sum(matched_quantity), 0.0) as volume, 
            count(id) as trade_count,
            coalesce(
                (
                    (select mq from last_trade where symbol = t.symbol)
                  - (select mq from first_trade where symbol = t.symbol)
                ) / (select mq from first_trade where symbol = t.symbol) * 100,
                0.0
            ) as change
        from trades t
        where create_date > :since
        group by symbol
        order by trade_count
        limit 1
    """
    )
    fun findByMostTrades(since: LocalDateTime): Mono<TradeVolumeStat>


    @Query(""" select t.trade_date As create_date,
            t.matched_quantity AS volume,
            t.matched_price AS matched_price,
            CASE
            WHEN t.maker_uuid = :user THEN t.maker_commission
            WHEN t.taker_uuid = :user THEN t.taker_commission
            END AS fee,
            CASE
            WHEN t.maker_uuid = :user THEN o1.side
            WHEN t.taker_uuid = :user THEN o2.side
            END AS side,
            t.matched_price * t.matched_quantity as transaction_price,
            substring(t.symbol, 0, position('_' in t.symbol) ) AS symbol
            FROM trades t
            INNER JOIN orders o1 ON t.maker_ouid = o1.ouid
            LEFT JOIN orders o2 ON t.taker_ouid = o2.ouid
            WHERE (t.maker_uuid = :user OR t.taker_uuid = :user)
            and (:startDate is null or trade_date >=:startDate) 
            and (:endDate is null or trade_date <=:endDate)
            
            union 
            
            select t.trade_date As create_date,
            t.matched_quantity AS volume,
            t.matched_price AS matched_price,
            CASE
            WHEN t.taker_uuid = :user THEN t.taker_commission
            WHEN t.maker_uuid = :user THEN t.maker_commission
            END AS fee,
            CASE
            WHEN t.taker_uuid = :user THEN o2.side
            WHEN t.maker_uuid = :user THEN o1.side
            END AS side,
            t.matched_price * t.matched_quantity as transaction_price,
            substring(t.symbol, 0, position('_' in t.symbol) ) AS symbol
            FROM trades t
            INNER JOIN orders o1 ON t.maker_ouid = o1.ouid
            LEFT JOIN orders o2 ON t.taker_ouid = o2.ouid
            WHERE (t.maker_uuid = :user OR t.taker_uuid = :user)
            and (:startDate is null or trade_date >=:startDate) 
            and (:endDate is null or trade_date <=:endDate)            
            
            order by create_date ASC offset :offset limit :limit  """)

    fun findTxOfTradesAsc(user: String, startDate: LocalDateTime?, endDate: LocalDateTime?, offset: Int?, limit: Int?): Flux<Transaction>



    @Query(""" select t.trade_date As create_date,
            t.matched_quantity AS volume,
            t.matched_price AS matched_price,
            CASE
            WHEN t.maker_uuid = :user THEN t.maker_commission
            WHEN t.taker_uuid = :user THEN t.taker_commission
            END AS fee,
            CASE
            WHEN t.maker_uuid = :user THEN o1.side
            WHEN t.taker_uuid = :user THEN o2.side
            END AS side,
            t.matched_price * t.matched_quantity as transaction_price,
            substring(t.symbol, 0, position('_' in t.symbol) ) AS symbol
            FROM trades t
            INNER JOIN orders o1 ON t.maker_ouid = o1.ouid
            LEFT JOIN orders o2 ON t.taker_ouid = o2.ouid
            WHERE (t.maker_uuid = :user OR t.taker_uuid = :user)
            and (:startDate is null or trade_date >=:startDate) 
            and (:endDate is null or trade_date <=:endDate)
            
            union 
            
            select t.trade_date As create_date,
            t.matched_quantity AS volume,
            t.matched_price AS matched_price,
            CASE
            WHEN t.taker_uuid = :user THEN t.taker_commission
            WHEN t.maker_uuid = :user THEN t.maker_commission
            END AS fee,
            CASE
            WHEN t.taker_uuid = :user THEN o2.side
            WHEN t.maker_uuid = :user THEN o1.side
            END AS side,
            t.matched_price * t.matched_quantity as transaction_price,
            substring(t.symbol, 0, position('_' in t.symbol) ) AS symbol
            FROM trades t
            INNER JOIN orders o1 ON t.maker_ouid = o1.ouid
            LEFT JOIN orders o2 ON t.taker_ouid = o2.ouid
            WHERE (t.maker_uuid = :user OR t.taker_uuid = :user)
            and (:startDate is null or trade_date >=:startDate) 
            and (:endDate is null or trade_date <=:endDate)            
            
            order by create_date DESC offset :offset limit :limit  """)

    fun findTxOfTradesDesc(user: String, startDate: LocalDateTime?, endDate: LocalDateTime?, offset: Int?, limit: Int?): Flux<Transaction>

    @Query(
        """
         WITH intervals AS (SELECT * FROM interval_generator((:startTime), (:endTime), :interval ::INTERVAL)),
        last_trade AS (
            SELECT DISTINCT ON (f.start_time) f.start_time,  f.end_time,  t.matched_price AS close_price FROM intervals f
            LEFT JOIN trades t ON t.create_date >= f.start_time AND t.create_date < f.end_time AND t.symbol = :symbol
            ORDER BY f.start_time, t.create_date DESC
        )
        SELECT
            i.end_time AS close_time,
            lt.close_price AS close_price
        FROM intervals i
        LEFT JOIN trades t
        ON t.create_date >= i.start_time AND t.create_date < i.end_time AND t.symbol = :symbol
        LEFT JOIN last_trade lt
        ON i.start_time = lt.start_time
        GROUP BY i.start_time, i.end_time, lt.close_price
        ORDER BY i.start_time;
        """
    )
    suspend fun getPriceTimeData(
        @Param("symbol")
        symbol: String,
        @Param("interval")
        interval: String,
        @Param("startTime")
        startTime: LocalDateTime,
        @Param("endTime")
        endTime: LocalDateTime,
    ): Flux<PriceTimeData>
}