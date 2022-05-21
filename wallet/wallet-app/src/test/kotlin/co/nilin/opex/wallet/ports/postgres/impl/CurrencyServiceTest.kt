package co.nilin.opex.wallet.ports.postgres.impl

import co.nilin.opex.wallet.ports.postgres.model.CurrencyModel
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.stubbing
import reactor.core.publisher.Mono

private class CurrencyServiceTest : CurrencyServiceTestBase() {
    @Test
    fun givenCorrectSymbol_whenGetCurrency_thenReturnCurrency(): Unit = runBlocking {
        stubbing(currencyRepository) {
            on { findBySymbol("ETH") } doReturn Mono.just(CurrencyModel("ETH", "Ethereum", 0.0001))
        }
        val c = currencyService.getCurrency("ETH")

        assertThat(c).isNotNull
        assertThat(c!!.getSymbol()).isEqualTo("ETH")
        assertThat(c.getName()).isEqualTo("Ethereum")
        assertThat(c.getPrecision()).isEqualTo(0.0001)
    }

    @Test
    fun givenNotExistCurrency_whenGetCurrency_thenReturnNull(): Unit = runBlocking {
        stubbing(currencyRepository) {
            on { findBySymbol("ETH") } doReturn Mono.empty()
        }
        val c = currencyService.getCurrency("ETH")

        assertThat(c).isNull()
    }

    @Test
    fun givenEmptySymbol_whenGetCurrency_thenReturnNull(): Unit = runBlocking {
        stubbing(currencyRepository) {
            on { findBySymbol("") } doReturn Mono.empty()
        }
        val c = currencyService.getCurrency("")

        assertThat(c).isNull()
    }
}