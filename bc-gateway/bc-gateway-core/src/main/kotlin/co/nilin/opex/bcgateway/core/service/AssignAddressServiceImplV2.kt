package co.nilin.opex.bcgateway.core.service

import co.nilin.opex.bcgateway.core.api.AssignAddressService
import co.nilin.opex.bcgateway.core.model.*
import co.nilin.opex.bcgateway.core.spi.*
import co.nilin.opex.bcgateway.core.utils.LoggerDelegate
import co.nilin.opex.common.OpexError
import org.slf4j.Logger
import org.springframework.beans.factory.annotation.Value
import org.springframework.transaction.annotation.Transactional
import java.time.LocalDateTime
import java.time.ZoneId

open class AssignAddressServiceImplV2(
        private val currencyHandler: CryptoCurrencyHandlerV2,
        private val assignedAddressHandler: AssignedAddressHandler,
        private val reservedAddressHandler: ReservedAddressHandler,
        private val chainLoader: ChainLoader
) : AssignAddressService {
    @Value("\${app.address.life-time.value}")
    private var lifeTime: Long? = null
    private val logger: Logger by LoggerDelegate()

    @Transactional
    override suspend fun assignAddress(user: String, currencyImplUuid: String): List<AssignedAddress> {
        logger.info(ZoneId.systemDefault().toString())
        val implsInfo = currencyHandler.fetchCurrencyImpls(FetchImpls(currencyImplUuid))?.imps?.firstOrNull()?.let {
            val requestedAddressType = chainLoader.fetchChainInfo(it.chain!!).addressTypes.map { it->it.type }.forEach { it.equals() }
            reservedAddressHandler.peekReservedAddress(requestedAddressType)
                    .flatMap { chain -> chain.addressTypes }
                    .distinct()
            val chainAddressTypeMap = HashMap<AddressType, MutableList<Chain>>()
            chains.forEach { chain ->
                chain.addressTypes.forEach { addressType ->
                    chainAddressTypeMap.putIfAbsent(addressType, mutableListOf())
                    chainAddressTypeMap.getValue(addressType).add(chain)
                }
            }
            val userAssignedAddresses = (assignedAddressHandler.fetchAssignedAddresses(user, addressTypes)).toMutableList()
            val result = mutableSetOf<AssignedAddress>()
            addressTypes.forEach { addressType ->
                val assigned = userAssignedAddresses.firstOrNull { assignAddress -> assignAddress.type == addressType }
                if (assigned != null) {
                    chainAddressTypeMap[addressType]?.forEach { chain ->
                        if (!assigned.chains.contains(chain)) {
                            assigned.chains.add(chain)
                        }
                    }
                    result.add(assigned)
                } else {
                    val reservedAddress = reservedAddressHandler.peekReservedAddress(addressType)
                    if (reservedAddress != null) {
                        val newAssigned = AssignedAddress(
                                user,
                                reservedAddress.address,
                                reservedAddress.memo,
                                addressType,
                                chainAddressTypeMap[addressType]!!,
                                lifeTime?.let { LocalDateTime.now().plusSeconds(lifeTime!!) }
                                        ?: null,
                                LocalDateTime.now(),
                                null,
                                AddressStatus.Assigned,
                                null
                        )
                        reservedAddressHandler.remove(reservedAddress)
                        result.add(newAssigned)
                    } else {
                        logger.info("No reserved address available for $addressType")
                        throw OpexError.ReservedAddressNotAvailable.exception()
                    }

                }
            }
            result.forEach { address ->
                assignedAddressHandler.persist(address)
                address.apply { id = null }
            }
            return result.toMutableList()
        }
    }
}
