package org.eddiecho.isolationleveldemo.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.function.BiConsumer;

/**
 * <a href="https://testcontainers.com/guides/testcontainers-container-lifecycle/#_using_singleton_containers">Singleton Containers Pattern</a>
 */
abstract class AbstractIntegrationTest {

    /**
     * MySQL Testcontainer with general query log enabled.
     * Logs all incoming SQL queries for inspection.
     * Reference: <a href="https://dev.mysql.com/doc/refman/8.4/en/query-log.html">MySQL General Query Log</a>
     */
    static final MySQLContainer<?> mysql = new MySQLContainer<>(DockerImageName.parse("mysql:8.0"))
            .withCommand("--general-log", "--general-log-file=/var/lib/mysql/general.log");

    static {
        mysql.start();
    }

    @BeforeAll
    static void startContainer() {
        System.setProperty("spring.datasource.url", mysql.getJdbcUrl());
        System.setProperty("spring.datasource.username", mysql.getUsername());
        System.setProperty("spring.datasource.password", mysql.getPassword());
    }

    final static BiConsumer<Number, Number> verifyReadValue = Assertions::assertEquals;

}
