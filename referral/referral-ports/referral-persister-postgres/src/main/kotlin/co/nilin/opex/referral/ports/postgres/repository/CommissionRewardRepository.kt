package co.nilin.opex.referral.ports.postgres.repository

import co.nilin.opex.referral.ports.postgres.dao.CommissionReward
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Repository
interface CommissionRewardRepository : ReactiveCrudRepository<CommissionReward, Long> {
    fun findByReferralCodeAndReferrerUuidAndReferentUuid(
        code: String?,
        referrerUuid: String?,
        referentUuid: String?
    ): Flux<CommissionReward>

    fun deleteByReferralCodeAndReferrerUuidAndReferentUuid(
        code: String?,
        referrerUuid: String?,
        referentUuid: String?
    ): Mono<Void>
}