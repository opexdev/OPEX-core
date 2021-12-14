package co.nilin.opex.referral.ports.postgres.impl

import co.nilin.opex.referral.core.model.CommissionReward
import co.nilin.opex.referral.core.spi.CommissionRewardHandler
import co.nilin.opex.referral.ports.postgres.repository.CommissionRewardRepository
import kotlinx.coroutines.reactive.awaitSingle
import org.springframework.stereotype.Service

@Service
class CommissionRewardHandlerImpl(
    private val commissionRewardRepository: CommissionRewardRepository
) : CommissionRewardHandler {
    override suspend fun findCommissions(
        referralCode: String?,
        referrerUuid: String?,
        referentUuid: String?
    ): List<CommissionReward> {
        return commissionRewardRepository.findByReferralCodeAndReferrerUuidAndReferentUuid(
            referralCode,
            referrerUuid,
            referentUuid
        ).map {
            CommissionReward(
                it.referrerUuid,
                it.referentUuid,
                it.referralCode,
                null,
                it.referrerShare,
                it.referentShare
            )
        }.collectList().awaitSingle()
    }

    override suspend fun deleteCommissions(referralCode: String?, referrerUuid: String?, referentUuid: String?) {
        commissionRewardRepository.deleteByReferralCodeAndReferrerUuidAndReferentUuid(referralCode, referrerUuid, referentUuid)
    }

    override suspend fun deleteCommissionById(id: Long) {
        commissionRewardRepository.deleteById(id)
    }
}
