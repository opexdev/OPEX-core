package co.nilin.opex.wallet.core.service

import co.nilin.opex.wallet.core.inout.TransferCommand
import co.nilin.opex.wallet.core.model.Amount
import co.nilin.opex.wallet.core.model.Currency
import co.nilin.opex.wallet.core.model.Wallet
import co.nilin.opex.wallet.core.model.WalletOwner
import co.nilin.opex.wallet.core.spi.TransactionManager
import co.nilin.opex.wallet.core.spi.WalletListener
import co.nilin.opex.wallet.core.spi.WalletManager
import co.nilin.opex.wallet.core.spi.WalletOwnerManager
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.anyString
import org.mockito.kotlin.*
import java.math.BigDecimal

private class TransferServiceTest {
    private val walletOwnerManager: WalletOwnerManager = mock()
    private val walletManager: WalletManager = mock()
    private val walletListener: WalletListener = mock()
    private val transactionManager: TransactionManager = mock()
    private val transferService: TransferService =
        TransferService(walletManager, walletListener, walletOwnerManager, transactionManager)

    private val currency = Currency("ETH", "Ethereum", BigDecimal.valueOf(0.0001))

    private val walletOwner = WalletOwner(
        2L,
        "fdf453d7-0633-4ec7-852d-a18148c99a82",
        "wallet",
        "1",
        isTradeAllowed = true,
        isWithdrawAllowed = true,
        isDepositAllowed = true
    )

    @Test
    fun givenWalletWithAllowedTransfer_whenTransfer_thenReturnTransferResultDetailed(): Unit = runBlocking {
        stubbing(walletOwnerManager) {
            onBlocking { isWithdrawAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
        }
        stubbing(walletManager) {
            onBlocking { isWithdrawAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { decreaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { increaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { findWalletById(20L) } doReturn Wallet(
                20L,
                walletOwner,
                Amount(currency, BigDecimal.valueOf(1)),
                currency,
                "main"
            )

        }
        stubbing(walletListener) {
            onBlocking { onWithdraw(any(), any(), any(), anyString(), any()) } doReturn Unit
            onBlocking { onDeposit(any(), any(), any(), any(), anyString(), any()) } doReturn Unit
        }
        stubbing(transactionManager) {
            onBlocking { save(any()) } doReturn "1"
        }
        val sourceWalletOwner = WalletOwner(
            2L,
            "fdf453d7-0633-4ec7-852d-a18148c99a82",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val sourceWallet = Wallet(
            20L,
            sourceWalletOwner,
            Amount(currency, BigDecimal.valueOf(1.5)),
            currency,
            "main",
        )
        val destWalletOwner = WalletOwner(
            3L,
            "e1950578-ef22-44e4-89f5-0b78feb03e2a",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val destWallet = Wallet(
            30L,
            destWalletOwner,
            Amount(currency, BigDecimal.valueOf(2.5)),
            currency,
            "main"
        )
        val transferCommand = TransferCommand(
            sourceWallet,
            destWallet,
            Amount(currency, BigDecimal.valueOf(0.5)),
            null,
            null,
            null
        )

        val result = transferService.transfer(transferCommand).transferResult

        assertThat(result).isNotNull
        assertThat(result.amount).isEqualTo(Amount(currency, BigDecimal.valueOf(0.5)))
        assertThat(result.sourceUuid).isEqualTo("fdf453d7-0633-4ec7-852d-a18148c99a82")
        assertThat(result.destUuid).isEqualTo("e1950578-ef22-44e4-89f5-0b78feb03e2a")
        assertThat(result.sourceWalletType).isEqualTo("main")
        assertThat(result.destWalletType).isEqualTo("main")
        assertThat(result.sourceBalanceBeforeAction).isEqualTo(Amount(currency, BigDecimal.valueOf(1.5)))
        assertThat(result.sourceBalanceAfterAction).isEqualTo(Amount(currency, BigDecimal.valueOf(1)))
    }

    @Test
    fun givenWalletWithOwnerWithdrawNotAllowed_whenTransfer_thenThrow(): Unit = runBlocking {
        stubbing(walletOwnerManager) {
            onBlocking {
                isWithdrawAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5))))
            } doReturn false
            onBlocking { isDepositAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
        }
        stubbing(walletManager) {
            onBlocking { isWithdrawAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { decreaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { increaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { findWalletById(20L) } doReturn Wallet(
                20L,
                walletOwner,
                Amount(currency, BigDecimal.valueOf(1)),
                currency,
                "main"
            )
        }
        stubbing(walletListener) {
            on {
                runBlocking {
                    onWithdraw(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
            on {
                runBlocking {
                    onDeposit(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        any(),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
        }
        stubbing(transactionManager) {
            onBlocking { save(any()) } doReturn "1"
        }
        val sourceWalletOwner = WalletOwner(
            2L,
            "fdf453d7-0633-4ec7-852d-a18148c99a82",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val sourceWallet = Wallet(
            20L,
            sourceWalletOwner,
            Amount(currency, BigDecimal.valueOf(1.5)),
            currency,
            "main"
        )
        val destWalletOwner = WalletOwner(
            3L,
            "e1950578-ef22-44e4-89f5-0b78feb03e2a",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val destWallet = Wallet(
            30L,
            destWalletOwner,
            Amount(currency, BigDecimal.valueOf(2.5)),
            currency,
            "main"
        )
        val transferCommand = TransferCommand(
            sourceWallet,
            destWallet,
            Amount(currency, BigDecimal.valueOf(0.5)),
            null,
            null,
            null
        )

        assertThatThrownBy { runBlocking { transferService.transfer(transferCommand) } }
    }

    @Test
    fun givenWalletWithWithdrawNotAllowed_whenTransfer_thenThrow(): Unit = runBlocking {
        stubbing(walletOwnerManager) {
            onBlocking { isWithdrawAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
        }
        stubbing(walletManager) {
            onBlocking { isWithdrawAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn false
            onBlocking { isDepositAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { decreaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { increaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { findWalletById(20L) } doReturn Wallet(
                20L,
                walletOwner,
                Amount(currency, BigDecimal.valueOf(1)),
                currency,
                "main"
            )
        }
        stubbing(walletListener) {
            on {
                runBlocking {
                    onWithdraw(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
            on {
                runBlocking {
                    onDeposit(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        any(),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
        }
        stubbing(transactionManager) {
            onBlocking { save(any()) } doReturn "1"
        }
        val sourceWalletOwner = WalletOwner(
            2L,
            "fdf453d7-0633-4ec7-852d-a18148c99a82",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val sourceWallet = Wallet(
            20L,
            sourceWalletOwner,
            Amount(currency, BigDecimal.valueOf(1.5)),
            currency,
            "main"
        )
        val destWalletOwner = WalletOwner(
            3L,
            "e1950578-ef22-44e4-89f5-0b78feb03e2a",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val destWallet = Wallet(
            30L,
            destWalletOwner,
            Amount(currency, BigDecimal.valueOf(2.5)),
            currency,
            "main"
        )
        val transferCommand = TransferCommand(
            sourceWallet,
            destWallet,
            Amount(currency, BigDecimal.valueOf(0.5)),
            null,
            null,
            null
        )

        assertThatThrownBy { runBlocking { transferService.transfer(transferCommand) } }
    }

    @Test
    fun givenWalletWithOwnerDepositNotAllowed_whenTransfer_thenThrow(): Unit = runBlocking {
        stubbing(walletOwnerManager) {
            onBlocking { isWithdrawAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn false
        }
        stubbing(walletManager) {
            onBlocking { isWithdrawAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { decreaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { increaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { findWalletById(20L) } doReturn Wallet(
                20L,
                walletOwner,
                Amount(currency, BigDecimal.valueOf(1)),
                currency,
                "main"
            )
        }
        stubbing(walletListener) {
            on {
                runBlocking {
                    onWithdraw(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
            on {
                runBlocking {
                    onDeposit(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        any(),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
        }
        stubbing(transactionManager) {
            onBlocking { save(any()) } doReturn "1"
        }
        val sourceWalletOwner = WalletOwner(
            2L,
            "fdf453d7-0633-4ec7-852d-a18148c99a82",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val sourceWallet = Wallet(
            20L,
            sourceWalletOwner,
            Amount(currency, BigDecimal.valueOf(1.5)),
            currency,
            "main"
        )
        val destWalletOwner = WalletOwner(
            3L,
            "e1950578-ef22-44e4-89f5-0b78feb03e2a",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val destWallet = Wallet(
            30L,
            destWalletOwner,
            Amount(currency, BigDecimal.valueOf(2.5)),
            currency,
            "main",
        )
        val transferCommand = TransferCommand(
            sourceWallet,
            destWallet,
            Amount(currency, BigDecimal.valueOf(0.5)),
            null,
            null,
            null
        )

        assertThatThrownBy { runBlocking { transferService.transfer(transferCommand) } }
    }

    @Test
    fun givenWalletWithDepositNotAllowed_whenTransfer_thenThrow(): Unit = runBlocking {
        stubbing(walletOwnerManager) {
            onBlocking { isWithdrawAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
        }
        stubbing(walletManager) {
            onBlocking { isWithdrawAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn false
            onBlocking { decreaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { increaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { findWalletById(20L) } doReturn Wallet(
                20L,
                walletOwner,
                Amount(currency, BigDecimal.valueOf(1)),
                currency,
                "main"
            )
        }
        stubbing(walletListener) {
            on {
                runBlocking {
                    onWithdraw(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
            on {
                runBlocking {
                    onDeposit(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        any(),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
        }
        stubbing(transactionManager) {
            onBlocking { save(any()) } doReturn "1"
        }
        val sourceWalletOwner = WalletOwner(
            2L,
            "fdf453d7-0633-4ec7-852d-a18148c99a82",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val sourceWallet = Wallet(
            20L,
            sourceWalletOwner,
            Amount(currency, BigDecimal.valueOf(1.5)),
            currency,
            "main"
        )
        val destWalletOwner = WalletOwner(
            3L,
            "e1950578-ef22-44e4-89f5-0b78feb03e2a",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val destWallet = Wallet(
            30L,
            destWalletOwner,
            Amount(currency, BigDecimal.valueOf(2.5)),
            currency,
            "main"
        )
        val transferCommand = TransferCommand(
            sourceWallet,
            destWallet,
            Amount(currency, BigDecimal.valueOf(0.5)),
            null,
            null,
            null
        )

        assertThatThrownBy { runBlocking { transferService.transfer(transferCommand) } }
    }

    @Test
    fun givenNoWallet_whenTransfer_thenThrow(): Unit = runBlocking {
        stubbing(walletOwnerManager) {
            onBlocking { isWithdrawAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(Amount(currency, BigDecimal.valueOf(0.5)))) } doReturn true
        }
        stubbing(walletManager) {
            onBlocking { isWithdrawAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { isDepositAllowed(any(), eq(BigDecimal.valueOf(0.5))) } doReturn true
            onBlocking { decreaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { increaseBalance(any(), eq(BigDecimal.valueOf(0.5))) } doReturn Unit
            onBlocking { findWalletById(20L) } doReturn null
        }
        stubbing(walletListener) {
            onBlocking {
                onWithdraw(
                    any(),
                    any(),
                    eq(Amount(currency, BigDecimal.valueOf(0.5))),
                    anyString(),
                    any()
                )
            } doReturn Unit
            on {
                runBlocking {
                    onDeposit(
                        any(),
                        any(),
                        eq(Amount(currency, BigDecimal.valueOf(0.5))),
                        any(),
                        anyString(),
                        any()
                    )
                }
            } doReturn Unit
        }
        stubbing(transactionManager) {
            onBlocking { save(any()) } doReturn "1"
        }
        val sourceWalletOwner = WalletOwner(
            2L,
            "fdf453d7-0633-4ec7-852d-a18148c99a82",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val sourceWallet = Wallet(
            20L,
            sourceWalletOwner,
            Amount(currency, BigDecimal.valueOf(1.5)),
            currency,
            "main"
        )
        val destWalletOwner = WalletOwner(
            3L,
            "e1950578-ef22-44e4-89f5-0b78feb03e2a",
            "wallet",
            "1",
            isTradeAllowed = true,
            isWithdrawAllowed = true,
            isDepositAllowed = true
        )
        val destWallet = Wallet(
            30L,
            destWalletOwner,
            Amount(currency, BigDecimal.valueOf(2.5)),
            currency,
            "main"
        )
        val transferCommand = TransferCommand(
            sourceWallet,
            destWallet,
            Amount(currency, BigDecimal.valueOf(0.5)),
            null,
            null,
            null
        )

        assertThatThrownBy { runBlocking { transferService.transfer(transferCommand) } }
    }
}