package org.eddiecho.isolationleveldemo.service;

import jakarta.persistence.EntityManager;
import lombok.extern.log4j.Log4j2;
import org.eddiecho.isolationleveldemo.model.Account;
import org.eddiecho.isolationleveldemo.repository.AccountRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

@Service
@Log4j2
public class AccountService {

    private final AccountRepository accountRepository;

    private final EntityManager entityManager;

    public AccountService(AccountRepository accountRepository, EntityManager entityManager) {
        this.accountRepository = accountRepository;
        this.entityManager = entityManager;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public Account create(String name) {
        Account account = new Account(name);
        return accountRepository.save(account);
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readUncommittedTransactionUpdateValueThenRollback(String name, long deposit, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValueThenRollback(name, deposit, waitForTransactionUpdateValue, waitForTransactionReadValue);
    }

    @Transactional(isolation = Isolation.READ_UNCOMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readUncommittedTransactionReadValue(String name, long expectedReadValue, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        this.readValue(name, expectedReadValue, waitForTransactionUpdateValue, waitForTransactionReadValue,verifyReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionUpdateValueThenRollback(String name, long deposit, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValueThenRollback(name, deposit, waitForTransactionUpdateValue, waitForTransactionReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionReadValue(String name, long expectedReadValue, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        this.readValue(name, expectedReadValue, waitForTransactionUpdateValue, waitForTransactionReadValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionReadValueTwice(String name, long expectedFirstReadValue, long expectedSecondReadValue, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        this.readValueTwice(name, expectedFirstReadValue, expectedSecondReadValue, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionUpdateValue(String name, long deposit, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValue(name, deposit, waitForTransactionReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionListValue(int expectedFirstResultSize, int expectedSecondResultSize, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        this.listValueTwice(expectedFirstResultSize, expectedSecondResultSize, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public void repeatableReadTransactionReadValueTwice(String name, long expectedFirstReadValue, long expectedSecondReadValue, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        this.readValueTwice(name, expectedFirstReadValue, expectedSecondReadValue, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public void repeatableReadTransactionUpdateValue(String name, long deposit, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValue(name, deposit, waitForTransactionReadValue);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public void repeatableReadTransactionListValue(int expectedFirstResultSize, int expectedSecondResultSize, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        this.listValueTwice(expectedFirstResultSize, expectedSecondResultSize, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.SERIALIZABLE, propagation = Propagation.REQUIRES_NEW)
    public void serializableTransactionListValue(int expectedFirstResultSize, int expectedSecondResultSize, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        this.listValueTwice(expectedFirstResultSize, expectedSecondResultSize, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.SERIALIZABLE, propagation = Propagation.REQUIRES_NEW)
    public void serializableTransactionUpdateValue(String name, long deposit, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValue(name, deposit, waitForTransactionReadValue);
    }

    private void updateValueThenRollback(String name, long deposit, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        Account account = accountRepository.findByName(name);
        log.info("updateValueThenRollback: [{}] - initial money: {}, depositing: {}", name, account.getMoney(), deposit);
        accountRepository.deposit(name, deposit);
        accountRepository.flush();
        waitForTransactionUpdateValue.countDown();
        waitForTransactionReadValue.await();
        throw new TransactionSystemException("transaction rollback");
    }

    private void readValue(String name, long expectedReadValue, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        waitForTransactionUpdateValue.await();
        Account account = accountRepository.findByName(name);
        verifyReadValue.accept(expectedReadValue, account.getMoney());
        log.info("readValue: [{}] - after flush from concurrent update - expected: {}, actual: {}", name, expectedReadValue, account.getMoney());
        waitForTransactionReadValue.countDown();
    }

    private void readValueTwice(String name, long expectedFirstReadValue, long expectedSecondReadValue, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        Account account = accountRepository.findByName(name);
        verifyReadValue.accept(expectedFirstReadValue, account.getMoney());
        log.info("readValueTwice: [{}] - before concurrent update - expected: {}, actual: {}", name, expectedFirstReadValue, account.getMoney());
        waitForTransactionReadValue.countDown();
        waitForTransactionUpdateValue.await();
        /*
          The Hibernate first-level cache (persistence context) provides application-level repeatable reads.
          To observe the actual effects of the database's transaction isolation level, we must clear the persistence context so that the entity is reloaded from the database.
         */
        entityManager.clear();
        account = accountRepository.findByName(name);
        verifyReadValue.accept(expectedSecondReadValue, account.getMoney());
        log.info("readValueTwice: [{}] - after commit from concurrent update - expected: {}, actual: {}", name, expectedSecondReadValue, account.getMoney());
    }

    private void updateValue(String name, long deposit, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        waitForTransactionReadValue.await(300, TimeUnit.MILLISECONDS);
        Account account = accountRepository.findByName(name);
        log.info("updateValue: [{}] - after concurrent read - initial money: {}, depositing: {}", name, account.getMoney(), deposit);
        accountRepository.deposit(name, deposit);
    }

    private void listValueTwice(int expectedFirstResultSize, int expectedSecondResultSize, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Number, Number> verifyReadValue) throws InterruptedException {
        List<Account> accounts = accountRepository.findAllByMoneyGreaterThan(0);
        verifyReadValue.accept(expectedFirstResultSize, accounts.size());
        log.info("listValueTwice: number of accounts which money are greater than 0 - before concurrent update - expected: {}, actual: {}", expectedFirstResultSize, accounts.size());
        waitForTransactionReadValue.countDown();
        waitForTransactionUpdateValue.await(300, TimeUnit.MILLISECONDS);
        accounts = accountRepository.findAllByMoneyGreaterThan(0);
        verifyReadValue.accept(expectedSecondResultSize, accounts.size());
        log.info("listValueTwice: number of accounts which money are greater than 0 - after concurrent update - expected: {}, actual: {}", expectedSecondResultSize, accounts.size());
    }



}
