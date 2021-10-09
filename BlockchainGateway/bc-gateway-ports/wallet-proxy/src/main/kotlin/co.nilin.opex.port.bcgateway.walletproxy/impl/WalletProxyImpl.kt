package co.nilin.opex.port.bcgateway.walletproxy.impl

import co.nilin.opex.bcgateway.core.spi.WalletProxy
import co.nilin.opex.wallet.core.inout.TransferResult
import kotlinx.coroutines.reactive.awaitFirst
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.ParameterizedTypeReference
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import java.math.BigDecimal
import java.net.URI

inline fun <reified T : Any> typeRef(): ParameterizedTypeReference<T> = object : ParameterizedTypeReference<T>() {}

@Component
class WalletProxyImpl(private val webClient: WebClient) : WalletProxy {
    @Value("\${app.wallet.url}")
    private lateinit var baseUrl: String

    override suspend fun transfer(uuid: String, symbol: String, amount: BigDecimal) {
        webClient.post()
            .uri(URI.create("$baseUrl/deposit/${amount}_$symbol/$uuid"))
            .header("Content-Type", "application/json")
            .retrieve()
            .onStatus({ t -> t.isError }, { it.createException() })
            .bodyToMono(typeRef<TransferResult>())
            .log()
            .awaitFirst()
    }
}
