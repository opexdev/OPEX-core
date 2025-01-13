package co.nilin.opex.market.core.inout

import java.math.BigDecimal
import java.time.LocalDateTime

data class PriceTime(
    val closeTime: LocalDateTime,
    val closePrice: BigDecimal,
)
