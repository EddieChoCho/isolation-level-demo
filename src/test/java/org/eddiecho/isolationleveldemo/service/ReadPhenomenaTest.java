package org.eddiecho.isolationleveldemo.service;

import org.eddiecho.isolationleveldemo.IsolationLevelDemoApplication;
import org.eddiecho.isolationleveldemo.repository.AccountRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.TransactionSystemException;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = IsolationLevelDemoApplication.class)
@Testcontainers(disabledWithoutDocker = true)
class ReadPhenomenaTest extends AbstractIntegrationTest{

    @Autowired
    private AccountService accountService;
    @Autowired
    private AccountRepository accountRepository;

    @AfterEach
    void cleanUp() {
        accountRepository.deleteAll();
    }

    /**
     * Dirty Reads with READ_UNCOMMITTED:
     *   - First transaction modify the value of accountB to 100
     *   - Then second transaction read the value of accountB as 100.
     *   - Then first transaction rollback.
     *   - The value read by the second transaction is now invalid.
     * Note:
     *   - While the SQL standard permits dirty reads at this isolation level, PostgresSQL does not allow it due to its implementation details.
     *   - See: <a href="https://www.postgresql.org/docs/current/transaction-iso.html">PostgreSQL: Transaction Isolation</a>
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
     * Prevent Dirty Reads with READ_COMMITTED
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
     * Nonrepeatable Reads with READ_COMMITTED:
     *  - First transaction read the value of the accountC is 0.
     *  - Then second transaction update the value of the accountC to 100.
     *  - Then second transaction commit.
     *  - When first transaction read the value of the accountC again.
     *  - The value is now 100, which is different from the initial read.
     */
    @Test
    void testNonrepeatableReadWithReadCommittedIsolation() throws InterruptedException {
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
     * Prevent Nonrepeatable Reads with REPEATABLE_READ:
     *   - First transaction read the value of the accountD is 0.
     *   - Then second transaction update the value of the accountD to 100.
     *   - Then second transaction commit.
     *   - When first transaction read the value of the accountD again.
     *   - The value will still be 0.
     */
    @Test
    void testPreventNonrepeatableReadWithRepeatableReadIsolation() throws InterruptedException {
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

    /**
     * Phantom Reads with READ_COMMITTED:
     *   - First transaction select all the accounts which money > 0, the result count is 0.
     *   - Then second transaction update the value of the accountE to 100.
     *   - Then second transaction commit.
     *   - When first transaction select all the accounts which money > 0 again, the result count is 1.
     * Note:
     *   - While the SQL standard permits phantom reads at READ_UNCOMMITTED, READ_COMMITTED, and REPEATABLE_READ levels.
     *   - Modern databases like MySQL and PostgreSQL typically prevent phantom reads in REPEATABLE_READ due to implementation details.
     *   - See: <a href="https://www.postgresql.org/docs/current/transaction-iso.html">PostgreSQL: Transaction Isolation</a>
     *   - See: <a href="https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html">MySQL: Transaction Isolation Levels</a>
     */
    @Test
    void testPhantomReadWithReadCommittedIsolation() throws InterruptedException {
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsCommit = new CountDownLatch(2);
        String account = "accountE";
        accountService.create(account);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                accountService.readCommittedTransactionListValue(0, 1, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
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
     * Prevent Phantom Reads with SERIALIZABLE:
     *   - First transaction select all the accounts which money > 0, the result count is 0.
     *   - Then second transaction update the value of the accountF to 100.
     *   - When first transaction select all the accounts which money > 0 again, the result count is still 0.
     *   - Then first transaction commit.
     *   - Then second transaction commit.
     */
    @Test
    void testPreventPhantomReadWithSerializableIsolation() throws InterruptedException {
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsCommit = new CountDownLatch(2);
        String account = "accountF";
        accountService.create(account);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                accountService.serializableTransactionListValue(0, 0, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTwoTransactionsCommit.countDown();
        });
        executorService.execute(() -> {
            try {
                accountService.serializableTransactionUpdateValue(account, 100, waitForTransactionReadValue);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            waitForTransactionUpdateValue.countDown();
            waitForTwoTransactionsCommit.countDown();
        });
        waitForTwoTransactionsCommit.await();
    }

    /**
     * MySQL prevent Phantom Reads with REPEATABLE_READ:
     *   - First transaction select all the accounts which money > 0, the result count is 0.
     *   - Then second transaction update the value of the accountF to 100.
     *   - Then second transaction commit.
     *   - When first transaction select all the accounts which money > 0 again, the result count is still 0.
     * See: <a href="https://dev.mysql.com/doc/refman/8.4/en/innodb-transaction-isolation-levels.html">17.7.2.1 Transaction Isolation Levels</a>
     */
    @Test
    void testPreventPhantomReadWithRepeatableReadIsolation() throws InterruptedException {
        CountDownLatch waitForTransactionReadValue = new CountDownLatch(1);
        CountDownLatch waitForTransactionUpdateValue = new CountDownLatch(1);
        CountDownLatch waitForTwoTransactionsCommit = new CountDownLatch(2);
        String account = "accountF";
        accountService.create(account);
        final ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(() -> {
            try {
                accountService.repeatableReadTransactionListValue(0, 0, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
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
