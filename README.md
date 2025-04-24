# Isolation Level Demo

This project demonstrates the behavior of various database isolation levels using MySQL and Testcontainers.
It is designed to help developers understand how different isolation levels impact transactional concurrency and data consistency.

## Preventable Phenomena and Transaction Isolation Levels

The table below summarizes which phenomena are allowed under each isolation level, based on the SQL standard (with notes on PostgreSQL and MySQL behavior):

| Isolation Level  | Dirty Read                          | Non-repeatable Read | Phantom Read                                |
|------------------|-------------------------------------|---------------------|---------------------------------------------|
| Read Uncommitted | ⚠️ Possible (but not in PostgreSQL) | ✅ Possible          | ✅ Possible                                  |
| Read Committed   | ❌ Not possible                      | ✅ Possible          | ✅ Possible                                  |
| Repeatable Read  | ❌ Not possible                      | ❌ Not possible      | ⚠️ Possible (but not in MySQL & PostgreSQL) |
| Serializable     | ❌ Not possible                      | ❌ Not possible      | ❌ Not possible                              |


The `AccountServiceTests` class contains the showcases of the three preventable phenomena: Dirty Reads, Nonrepeatable Reads, and Phantom Reads.