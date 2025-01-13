package co.nilin.opex.market.app.data

data class SparkLineDataResponse(
    val symbol: String,
    val isTrendUp: Boolean,
    val svgData: String
)
