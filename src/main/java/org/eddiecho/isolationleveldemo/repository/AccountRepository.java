package org.eddiecho.isolationleveldemo.repository;

import org.eddiecho.isolationleveldemo.model.Account;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface AccountRepository extends JpaRepository<Account, Long> {

    Account findByName(String name);

    @Modifying
    @Query("update Account account set account.money = account.money + ?2 where account.name = ?1")
    void deposit(String name, long money);
}