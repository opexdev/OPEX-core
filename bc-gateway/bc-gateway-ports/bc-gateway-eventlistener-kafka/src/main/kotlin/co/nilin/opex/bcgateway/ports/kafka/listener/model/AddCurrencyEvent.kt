package co.nilin.opex.bcgateway.ports.kafka.listener.model

import java.math.BigDecimal

data class AddCurrencyEvent(
    val name: String,
    val symbol: String,
    val precision: BigDecimal
) : AdminEvent()