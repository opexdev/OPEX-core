package co.nilin.opex.api.ports.postgres.impl

import co.nilin.opex.api.ports.postgres.dao.OrderRepository
import co.nilin.opex.api.ports.postgres.dao.OrderStatusRepository
import co.nilin.opex.api.ports.postgres.dao.TradeRepository
import co.nilin.opex.api.ports.postgres.impl.testfixtures.Valid
import kotlinx.coroutines.flow.count
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.stubbing
import reactor.core.publisher.Mono

class UserQueryHandlerTest {
    private val orderRepository: OrderRepository = mock()
    private val tradeRepository: TradeRepository = mock()
    private val orderStatusRepository: OrderStatusRepository = mock()
    private val userQueryHandler = UserQueryHandlerImpl(orderRepository, tradeRepository, orderStatusRepository)

    @Test
    fun given_whenAllOrders_then(): Unit = runBlocking {
        stubbing(orderRepository) {
            on {
                findByUuidAndSymbolAndTimeBetween(
                    Valid.PRINCIPAL.name,
                    Valid.ALL_ORDER_REQUEST.symbol,
                    Valid.ALL_ORDER_REQUEST.startTime,
                    Valid.ALL_ORDER_REQUEST.endTime
                )
            } doReturn flow {
                emit(Valid.ORDER_MODEL)
            }
        }

        val queryOrderResponses = userQueryHandler.allOrders(Valid.PRINCIPAL, Valid.ALL_ORDER_REQUEST)

        assertThat(queryOrderResponses).isNotNull
        assertThat(queryOrderResponses.count()).isEqualTo(1)
        assertThat(queryOrderResponses.first()).isEqualTo(Valid.QUERY_ORDER_RESPONSE)
    }

    @Test
    fun given_whenAllTrades_then(): Unit = runBlocking {
        stubbing(tradeRepository) {
            on {
                findByUuidAndSymbolAndTimeBetweenAndTradeIdGreaterThan(
                    Valid.PRINCIPAL.name,
                    Valid.TRADE_REQUEST.symbol,
                    1,
                    Valid.TRADE_REQUEST.startTime,
                    Valid.TRADE_REQUEST.endTime
                )
            } doReturn flow {
                emit(Valid.TRADE_MODEL)
            }
        }

        val tradeResponses = userQueryHandler.allTrades(Valid.PRINCIPAL, Valid.TRADE_REQUEST)

        assertThat(tradeResponses).isNotNull
        assertThat(tradeResponses.count()).isEqualTo(1)
    }

    @Test
    fun given_whenOpenOrders_then(): Unit = runBlocking {
        stubbing(orderRepository) {
            on {
                findByUuidAndSymbolAndStatus(Valid.PRINCIPAL.name, "ETH_USDT", listOf(0))
            } doReturn flow {
                emit(Valid.ORDER_MODEL)
            }
        }

        val queryOrderResponses = userQueryHandler.openOrders(Valid.PRINCIPAL, "ETH_USDT")

        assertThat(queryOrderResponses).isNotNull
        assertThat(queryOrderResponses.count()).isEqualTo(1)
    }

    @Test
    fun given_whenQueryOrder_then(): Unit = runBlocking {
        stubbing(orderRepository) {
            on {
                findBySymbolAndClientOrderId("ETH_USDT", "id")
            } doReturn Mono.just(Valid.ORDER_MODEL)
            on {
                findBySymbolAndOrderId("ETH_USDT", 1)
            } doReturn Mono.just(Valid.ORDER_MODEL)
        }
        stubbing(orderStatusRepository) {
            on {
                findMostRecentByOUID("f1167d30-ccc0-4f86-ab5d-dd24aa3250df")
            } doReturn Mono.just(Valid.ORDER_STATUS_MODEL)
        }

        val queryOrderResponse = userQueryHandler.queryOrder(Valid.PRINCIPAL, Valid.QUERY_ORDER_REQUEST)
        assertThat(queryOrderResponse).isNotNull
    }
}
