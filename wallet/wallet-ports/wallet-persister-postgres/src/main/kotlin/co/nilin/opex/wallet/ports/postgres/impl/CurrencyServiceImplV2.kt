package co.nilin.opex.wallet.ports.postgres.impl

import co.nilin.opex.common.OpexError
import co.nilin.opex.wallet.core.inout.CurrenciesCommand
import co.nilin.opex.wallet.core.inout.CurrencyCommand
//import co.nilin.opex.wallet.core.model.Currencies
//import co.nilin.opex.wallet.core.model.Currency
//import co.nilin.opex.wallet.core.model.CurrencyImp
import co.nilin.opex.wallet.core.model.FetchCurrency
import co.nilin.opex.wallet.core.spi.CurrencyServiceManager
import co.nilin.opex.wallet.ports.postgres.dao.CurrencyRepositoryV2
import co.nilin.opex.wallet.ports.postgres.model.NewCurrencyModel
import co.nilin.opex.wallet.ports.postgres.util.toCommand
import co.nilin.opex.wallet.ports.postgres.util.toModel
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.util.stream.Collectors

@Service("newVersion")
class CurrencyServiceImplV2(val currencyRepository: CurrencyRepositoryV2) : CurrencyServiceManager {


    override suspend fun createNewCurrency(request: CurrencyCommand): CurrencyCommand? {
        return loadCurrency(FetchCurrency(symbol = request.symbol))?.awaitFirstOrNull()?.let {
            throw OpexError.CurrencyIsExist.exception()
        } ?: run {
            return doSave(request.toModel())?.toCommand()
        }
    }


    override suspend fun updateCurrency(request: CurrencyCommand): CurrencyCommand? {
        return loadCurrency(FetchCurrency(uuid = request.uuid))
                ?.awaitFirstOrNull()?.let {
                    doSave(it.toCommand().updateTo(request).toModel().apply { id = it.id })?.toCommand()
                } ?: throw OpexError.CurrencyNotFound.exception()

    }

    override suspend fun prepareCurrencyToBeACryptoCurrency(request: String): CurrencyCommand? {
        return loadCurrency(FetchCurrency(uuid = request))?.awaitFirstOrNull()?.let {
            if (it.isCryptoCurrency == false)
                return doSave(it.apply { isCryptoCurrency = true })?.toCommand()
            it.toCommand()
        } ?: throw OpexError.CurrencyNotFound.exception()
    }

    override suspend fun deleteCurrencies(request: FetchCurrency) {
        loadCurrency(request)?.map {
            currencyRepository.delete(it)
        }?.awaitFirstOrNull()
    }

    private suspend fun loadCurrency(request: FetchCurrency): Mono<NewCurrencyModel>? {
        return currencyRepository.fetchCurrency(request.uuid, request.symbol)
    }

    private suspend fun loadCurrencies(request: FetchCurrency): Flux<NewCurrencyModel>? {
        return currencyRepository.fetchSemiCurrencies(request.uuid, request.symbol, request.name)
    }


    private suspend fun doSave(request: NewCurrencyModel): NewCurrencyModel? {
        return currencyRepository.save(request).awaitFirstOrNull()

    }


    override suspend fun fetchCurrencs(request: FetchCurrency): CurrenciesCommand? {
        return CurrenciesCommand(loadCurrencies(request)?.map { it.toCommand() }
                ?.collect(Collectors.toList())?.awaitFirstOrNull())
    }

    override suspend fun fetchCurrency(request: FetchCurrency): CurrencyCommand? {
        return loadCurrency(request)?.awaitFirstOrNull()?.toCommand()
    }


}