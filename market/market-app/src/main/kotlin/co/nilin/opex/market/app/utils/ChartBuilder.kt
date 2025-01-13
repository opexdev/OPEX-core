import org.jfree.chart.ChartFactory
import org.jfree.chart.JFreeChart
import org.jfree.chart.plot.PlotOrientation
import org.jfree.chart.plot.XYPlot
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer
import org.jfree.data.xy.XYSeries
import org.jfree.data.xy.XYSeriesCollection
import org.jfree.graphics2d.svg.SVGGraphics2D
import java.awt.Rectangle
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.Base64

fun createLineChart(prices: List<BigDecimal>, times: List<LocalDateTime>): String {
    val series = XYSeries("Price").apply {
        prices.zip(times).forEach { (price, time) ->
            add(time.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli(), price.toDouble())
        }
    }
    val dataset = XYSeriesCollection().apply {
        addSeries(series)
    }
    val chart = ChartFactory.createXYLineChart(
        null, // Chart title
        null, // X-axis label
        null, // Y-axis label
        dataset, // Data
        PlotOrientation.VERTICAL,
        false,
        false,
        false
    )
    val plot: XYPlot = chart.xyPlot
    val renderer = XYLineAndShapeRenderer(true, false)
    // Set chart color
    renderer.setSeriesPaint(0, java.awt.Color.WHITE)
    // Set axis ranges to keep the chart logical
    plot.rangeAxis.range = org.jfree.data.Range(
        prices.minOrNull()?.toDouble()?.times(0.99) ?: 0.0,
        prices.maxOrNull()?.toDouble() ?: 0.0
    )
    // Remove gridlines, axis, and background
    plot.renderer = renderer
    plot.isDomainGridlinesVisible = false
    plot.isRangeGridlinesVisible = false
    plot.backgroundPaint = null
    plot.domainAxis.isVisible = false
    plot.rangeAxis.isVisible = false
    plot.isOutlineVisible = false
    chart.backgroundPaint = null

    return chartToSvgString(chart, 100, 35)
}

fun chartToSvgString(chart: JFreeChart, width: Int, height: Int): String {
    val svg = SVGGraphics2D(width, height)
    chart.draw(svg, Rectangle(width, height))
    val svgString = svg.svgElement
    svg.dispose()
    val base64SvgString = Base64.getEncoder().encodeToString(svgString.toByteArray(Charsets.UTF_8))
    return base64SvgString
}
