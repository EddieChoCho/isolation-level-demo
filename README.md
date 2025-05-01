# Isolation Level Demo

This project demonstrates the behavior of various database isolation levels using MySQL and Testcontainers.
It is designed to help developers understand how different isolation levels impact transactional concurrency and data consistency.

## Preventable Phenomena and Transaction Isolation Levels

The table below summarizes which phenomena are allowed under each isolation level, based on the SQL standard (with notes on PostgreSQL and MySQL behavior):

| Isolation Level  | Dirty Read                          | Non-repeatable Read | Phantom Read                                                 |
|------------------|-------------------------------------|---------------------|--------------------------------------------------------------|
| Read Uncommitted | ⚠️ Possible (but not in PostgreSQL) | ✅ Possible          | ✅ Possible                                                   |
| Read Committed   | ❌ Not possible                      | ✅ Possible          | ✅ Possible                                                   |
| Repeatable Read  | ❌ Not possible                      | ❌ Not possible      | ⚠️ Possible (but not in PostgreSQL & MySQL with gap locking) |
| Serializable     | ❌ Not possible                      | ❌ Not possible      | ❌ Not possible                                               |


The `ReadPhenomenaTest` class contains the showcases of the three preventable phenomena: Dirty Reads, Nonrepeatable Reads, and Phantom Reads.

## Hibernate's Application-level Repeatable Read

### REPEATABLE_READ Isolation Level vs. Application-Level Repeatable Read
1. The REPEATABLE_READ Isolation level prevents not only non-repeatable reads but also dirty reads. 
   In modern databases like PostgreSQL, it can also prevent phantom reads. 
   On the other hand, if application-level repeatable reads is used with the READ_UNCOMMITTED isolation level, dirty reads might still occur because the underlying database allows them. 
   Additionally, application-level repeatable reads cannot prevent phantom reads by itself.

2. The REPEATABLE_READ Isolation level ensures that all reads in a transaction see data from the same snapshot.
   In contrast, application-level repeatable reads only guarantees that an entity read multiple times within the same Hibernate session will return the same in-memory object (from the first-level cache). 
   When used with the READ_UNCOMMITTED or READ_COMMITTED isolation level, this can lead to unexpected results if a transaction reads some parts of the data that have been updated by other transactions, while reading other parts from the session cache. (see [this example](https://stackoverflow.com/questions/25106636/strategies-for-dealing-with-concurrency-issues-caused-by-stale-domain-objects-g))

The `ApplicationLevelRepeatableReadsTest` class contains the showcases of Hibernate's application-level repeatable reads.


