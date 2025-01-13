package co.nilin.opex.market.ports.postgres.model

import java.math.BigDecimal
import java.time.LocalDateTime

data class PriceTimeData(
    val closeTime: LocalDateTime,
    val closePrice: BigDecimal?,
)