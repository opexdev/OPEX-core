package co.nilin.opex.profile.core.data.linkedbankAccount

data class VerifyLinkedAccountRequest(val verified: Boolean, var description: String?, var accountId: String?, var verifier: String?)