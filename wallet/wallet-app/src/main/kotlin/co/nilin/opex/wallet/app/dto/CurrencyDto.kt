package co.nilin.opex.wallet.app.dto

import co.nilin.opex.common.OpexError
import co.nilin.opex.wallet.core.inout.CryptoCurrencyCommand
import co.nilin.opex.wallet.core.inout.CurrencyCommand
import co.nilin.opex.wallet.core.inout.DepositMethod
import co.nilin.opex.wallet.core.inout.WithdrawMethod
import org.modelmapper.ModelMapper
import java.math.BigDecimal
import java.util.*

data class CurrencyDto(
        var symbol: String? = null,
        var uuid: String? = UUID.randomUUID().toString(),
        var name: String,
        var precision: BigDecimal,
        var title: String? = null,
        var alias: String? = null,
        var icon: String? = null,
        var isTransitive: Boolean? = false,
        var isActive: Boolean? = true,
        var sign: String? = null,
        var description: String? = null,
        var shortDescription: String? = null,
        var withdrawAllowed: Boolean? = true,
        var depositAllowed: Boolean? = true,
        var withdrawFee: BigDecimal? = BigDecimal.ZERO,
        var depositMethods: List<DepositMethod>? = null,
        var withdrawMethods: List<WithdrawMethod>? = null,
        var externalUrl: String? = null,
        var isCryptoCurrency: Boolean? = false,
        var impls: List<CryptoCurrencyCommand>? = null,
        var withdrawMin: BigDecimal? = BigDecimal.ZERO,

        ) {

    // Separated them just to support having "id" field in currency table.
    //Now it is unnecessary
    fun toCommand(): CurrencyCommand {

        return CurrencyCommand(
                symbol!!,
                uuid,
                name,
                precision,
                title,
                alias,
                icon,
                isTransitive,
                isActive,
                sign,
                description,
                shortDescription,
                withdrawAllowed,
                depositAllowed,
                withdrawFee,
                depositMethods,
                withdrawMethods,
                externalUrl,
                isCryptoCurrency,
                impls,
                withdrawMin)
    }
}


data class CurrenciesDto(var currencies: List<CurrencyDto>?)
