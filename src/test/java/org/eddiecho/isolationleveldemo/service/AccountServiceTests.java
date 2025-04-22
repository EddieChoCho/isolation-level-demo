package org.eddiecho.isolationleveldemo.service;

import lombok.extern.log4j.Log4j2;
import org.eddiecho.isolationleveldemo.IsolationLevelDemoApplication;
import org.eddiecho.isolationleveldemo.repository.AccountRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.TransactionSystemException;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

@Log4j2
@SpringBootTest(classes = IsolationLevelDemoApplication.class)
@Testcontainers(disabledWithoutDocker = true)
class AccountServiceTests {

    @Container
    private static final MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"));

    @Autowired
    private AccountService accountService;
    @Autowired
    private AccountRepository accountRepository;

    @BeforeAll
    static void startContainer() {
        System.setProperty("spring.datasource.url", mysql.getJdbcUrl());
        System.setProperty("spring.datasource.username", mysql.getUsername());
        System.setProperty("spring.datasource.password", mysql.getPassword());
    }

    @AfterEach
    void cleanUp() {
        accountRepository.deleteAll();
    }

    private final BiConsumer<Long, Long> verifyReadValue = Assertions::assertEquals;

    /**
     * Dirty Read with READ_UNCOMMITTED:
     *   - First transaction modify the value of accountB to 100
     *   - Then second transaction read the value of accountB as 100.
     *   - Then first transaction rollback.
     *   - The value read by the second transaction is now invalid.
     * Note:
     *   - While the SQL standard permits dirty reads at this isolation level, PostgresSQL does not allow it due to its implementation details.
     *   - <a href="https://www.postgresql.org/docs/current/transaction-iso.html">reference</a>
     */
    @Test
    void testDirtyReadWithReadUncommittedIsolation() throws InterruptedException {
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsExecute = new CountDownLatch(2);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        String account = "accountA";
        accountService.create(account);
        executorService.execute(() -> {
            TransactionSystemException exception = assertThrows(TransactionSystemException.class, () -> accountService.readUncommittedTransactionUpdateValueThenRollback(account, 100,  waitForTransactionUpdateValue, waitForTransactionReadValue));
            assertEquals("transaction rollback", exception.getMessage());
            waitForTwoTransactionsExecute.countDown();

        });
        executorService.execute(() -> {
            try {
                accountService.readUncommittedTransactionReadValue(account, 100,  waitForTransactionUpdateValue, waitForTransactionReadValue, verifyReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTwoTransactionsExecute.countDown();
        });
        waitForTwoTransactionsExecute.await();
    }

    /**
     * Prevent Dirty Read with READ_COMMITTED
     *  - First transaction modify the value of accountB to 100
     *  - Then second transaction read the value of accountB as 0.
     *  - Then first transaction rollback
     *  - The value read by the second transaction is valid.
     */
    @Test
    void testPreventDirtyReadWithReadCommittedIsolation() throws InterruptedException {
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsExecute = new CountDownLatch(2);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        String account = "accountB";
        accountService.create(account);
        executorService.execute(() -> {
            TransactionSystemException exception = assertThrows(TransactionSystemException.class, () -> accountService.readCommittedTransactionUpdateValueThenRollback(account, 100, waitForTransactionUpdateValue, waitForTransactionReadValue));
            assertEquals("transaction rollback", exception.getMessage());
            waitForTwoTransactionsExecute.countDown();

        });
        executorService.execute(() -> {
            try {
                accountService.readCommittedTransactionReadValue(account,  0,  waitForTransactionUpdateValue, waitForTransactionReadValue, verifyReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTwoTransactionsExecute.countDown();
        });
        waitForTwoTransactionsExecute.await();
    }

    /**
     * Unrepeatable Read with READ_COMMITTED:
     *  - First transaction read the value of the accountC is 0.
     *  - Then second transaction update the value of the accountC to 100.
     *  - Then second transaction commit.
     *  - When first transaction read the value of the accountC again.
     *  - The value is now 100, which is different from the initial read.
     */
    @Test
    void testUnrepeatableReadWithReadCommittedIsolation() throws InterruptedException {
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsCommit = new CountDownLatch(2);
        String account = "accountC";
        accountService.create(account);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                accountService.readCommittedTransactionReadValueTwice(account, 0, 100, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
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
     * Prevent Unrepeatable Read with REPEATABLE_READ:
     *  - First transaction read the value of the accountC is 0.
     *   - Then second transaction update the value of the accountC to 100.
     *   - Then second transaction commit.
     *   - When first transaction read the value of the accountC again.
     *   - The value will still be 0.
     */
    @Test
    void testPreventUnrepeatableReadWithRepeatableReadIsolation() throws InterruptedException {
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsCommit = new CountDownLatch(2);
        String account = "accountD";
        accountService.create(account);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                accountService.repeatableReadTransactionReadValueTwice(account, 0, 0, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTwoTransactionsCommit.countDown();
        });
        executorService.execute(() -> {
            try {
                accountService.repeatableReadTransactionUpdateValue(account, 100, waitForTransactionReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTransactionUpdateValue.countDown();
            waitForTwoTransactionsCommit.countDown();
        });
        waitForTwoTransactionsCommit.await();
    }

}
