package org.eddiecho.isolationleveldemo.service;

import org.eddiecho.isolationleveldemo.IsolationLevelDemoApplication;
import org.eddiecho.isolationleveldemo.model.Account;
import org.eddiecho.isolationleveldemo.repository.AccountRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@SpringBootTest(classes = IsolationLevelDemoApplication.class)
@Testcontainers(disabledWithoutDocker = true)
class ApplicationLevelRepeatableReadsTest extends AbstractIntegrationTest {

    @Autowired
    private AccountRepository accountRepository;
    @Autowired
    private AccountService accountService;

    @AfterEach
    void cleanUp() {
        accountRepository.deleteAll();
    }

    /**
     * Prevent Nonrepeatable Reads with READ_COMMITTED & Hibernate's Application-level Repeatable Reads:
     *   - First transaction read the value of the accountG is 0.
     *   - Then second transaction update the value of the accountG to 100.
     *   - Then second transaction commit.
     *   - When first transaction read the value of the accountG again.
     *   - The value will still be 0.
     */
    @Test
    void testPreventNonrepeatableReadWithApplicationLevelRepeatableReads() throws InterruptedException {
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsCommit = new CountDownLatch(2);
        String account = "accountG";
        accountService.create(account);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                accountService.applicationLevelRepeatableReadsReadValueTwice(account, 0, 0, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTwoTransactionsCommit.countDown();
        });
        executorService.execute(() -> {
            try {
                accountService.readCommittedTransactionUpdateValue(account, 100, waitForTransactionReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTransactionUpdateValue.countDown();
            waitForTwoTransactionsCommit.countDown();
        });
        waitForTwoTransactionsCommit.await();
    }

    /**
     * Application-level Repeatable Reads Issue with READ_COMMITTED
     * - There are two accounts in the database, which names are "account1", and "account2"
     *   - First transaction read the name of the account1 is "account1"
     *   - Then second transaction update the name of the account1 to "account3".
     *   - Then second transaction commit.
     *   - When first transaction list all the account
     *   - The names of the accounts are "account1", and "account1"
     * Note:
     *   - See: <a href="https://stackoverflow.com/questions/25106636/strategies-for-dealing-with-concurrency-issues-caused-by-stale-domain-objects-g">unexpected result</a>
     */
    @Test
    void testApplicationLevelRepeatableReadsIssue() throws InterruptedException {
        CountDownLatch waitForRead = new CountDownLatch(1);
        CountDownLatch waitForWrite = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactions = new CountDownLatch(2);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        String oldAccount1Name = "account1";
        String oldAccount2Name = "account2";
        String newAccount1Name = "account3";
        String newAccount2Name = "account1";
        Account account1 = accountService.create(oldAccount1Name);
        Account account2 = accountService.create(oldAccount2Name);
        executorService.execute(() -> {
            try {
                List<Account> accounts = accountService.applicationLevelRepeatableReadsReadValueThenListAllValues(waitForRead, waitForWrite, oldAccount1Name);
                assertThat(accounts).extracting("id", "name")
                        .contains(tuple(account1.getId(), oldAccount1Name),
                                tuple(account2.getId(), newAccount2Name));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTwoTransactions.countDown();
        });
        executorService.execute(() -> {
            try {
                accountService.readCommitedTransactionUpdateAccountNames(waitForRead, oldAccount1Name, oldAccount2Name, newAccount1Name, newAccount2Name);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForWrite.countDown();
            waitForTwoTransactions.countDown();
        });
        waitForTwoTransactions.await();
    }

    /**
     * Prevent Application-level Repeatable Reads Issue with REPEATABLE_READ
     * - There are two accounts in the database, which names are "account1", and "account2"
     *   - First transaction read the name of the account1 is "account1"
     *   - Then second transaction update the name of the account1 to "account3".
     *   - Then second transaction commit.
     *   - When first transaction list all the account
     *   - The names of the accounts are "account1", and "account2"
     */
    @Test
    void testPreventApplicationLevelRepeatableReadsIssueWithRepeatableReadIsolation() throws InterruptedException {
        CountDownLatch waitForRead = new CountDownLatch(1);
        CountDownLatch waitForWrite = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactions = new CountDownLatch(2);
        String oldAccount1Name = "account1";
        String oldAccount2Name = "account2";
        String newAccount1Name = "account3";
        String newAccount2Name = "account1";
        Account account1 = accountService.create(oldAccount1Name);
        Account account2 = accountService.create(oldAccount2Name);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                List<Account> accounts = accountService.repeatableReadTransactionReadValueThenListAllValues(waitForRead, waitForWrite, oldAccount1Name);
                assertThat(accounts).extracting("id", "name")
                        .contains(tuple(account1.getId(), oldAccount1Name),
                                tuple(account2.getId(), oldAccount2Name));
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTwoTransactions.countDown();
        });
        executorService.execute(() -> {
            try {
                accountService.repeatableReadTransactionUpdateAccountNames(waitForRead, oldAccount1Name, oldAccount2Name, newAccount1Name, newAccount2Name);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForWrite.countDown();
            waitForTwoTransactions.countDown();
        });
        waitForTwoTransactions.await();
    }


}
