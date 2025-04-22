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

import java.util.concurrent.CountDownLatch;
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
    public void readUncommittedTransactionReadValue(String name, long expectedReadValue, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue, BiConsumer<Long, Long> verifyReadValue) throws InterruptedException {
        this.readValue(name, expectedReadValue, waitForTransactionUpdateValue, waitForTransactionReadValue,verifyReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionUpdateValueThenRollback(String name, long deposit, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValueThenRollback(name, deposit, waitForTransactionUpdateValue, waitForTransactionReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionReadValue(String name, long expectedReadValue, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue, BiConsumer<Long, Long> verifyReadValue) throws InterruptedException {
        this.readValue(name, expectedReadValue, waitForTransactionUpdateValue, waitForTransactionReadValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionReadValueTwice(String name, long expectedFirstReadValue, long expectedSecondReadValue, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Long, Long> verifyReadValue) throws InterruptedException {
        this.readValueTwice(name, expectedFirstReadValue, expectedSecondReadValue, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.READ_COMMITTED, propagation = Propagation.REQUIRES_NEW)
    public void readCommittedTransactionUpdateValue(String name, long deposit, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValue(name, deposit, waitForTransactionReadValue);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public void repeatableReadTransactionReadValueTwice(String name, long expectedFirstReadValue, long expectedSecondReadValue, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Long, Long> verifyReadValue) throws InterruptedException {
        this.readValueTwice(name, expectedFirstReadValue, expectedSecondReadValue, waitForTransactionReadValue, waitForTransactionUpdateValue, verifyReadValue);
    }

    @Transactional(isolation = Isolation.REPEATABLE_READ, propagation = Propagation.REQUIRES_NEW)
    public void repeatableReadTransactionUpdateValue(String name, long deposit, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        this.updateValue(name, deposit, waitForTransactionReadValue);
    }

    private void updateValueThenRollback(String name, long deposit, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        Account account = accountRepository.findByName(name);
        log.info("firstTransactionDeposit: money of the account: {}", account.getMoney());
        accountRepository.deposit(name, deposit);
        accountRepository.flush();
        waitForTransactionUpdateValue.countDown();
        waitForTransactionReadValue.await();
        throw new TransactionSystemException("transaction rollback");
    }

    private void readValue(String name, long moneyShouldBeShown, CountDownLatch waitForTransactionUpdateValue, CountDownLatch waitForTransactionReadValue, BiConsumer<Long, Long> verifyReadValue) throws InterruptedException {
        waitForTransactionUpdateValue.await();
        Account account = accountRepository.findByName(name);
        verifyReadValue.accept(moneyShouldBeShown, account.getMoney());
        log.info("secondTransactionDeposit: money of the account: {}", account.getMoney());
        waitForTransactionReadValue.countDown();
    }

    private void readValueTwice(String name, long expectedFirstReadValue, long expectedSecondReadValue, CountDownLatch waitForTransactionReadValue, CountDownLatch waitForTransactionUpdateValue, BiConsumer<Long, Long> verifyReadValue) throws InterruptedException {
        Account account = accountRepository.findByName(name);
        verifyReadValue.accept(expectedFirstReadValue, account.getMoney());
        log.info("firstTransactionRead: money of the account: {}", account.getMoney());
        waitForTransactionReadValue.countDown();
        waitForTransactionUpdateValue.await();
        entityManager.clear();
        account = accountRepository.findByName(name);
        verifyReadValue.accept(expectedSecondReadValue, account.getMoney());
        log.info("firstTransactionRead, read again: money of the account: {}", account.getMoney());
    }

    private void updateValue(String name, long deposit, CountDownLatch waitForTransactionReadValue) throws InterruptedException {
        waitForTransactionReadValue.await();
        Account account = accountRepository.findByName(name);
        log.info("secondTransactionUpdate: money of the account: {}", account.getMoney());
        accountRepository.deposit(name, deposit);
    }



}
