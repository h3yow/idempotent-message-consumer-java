package com.github.h3yow.idempotentmessageconsumerjava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * @author Vincent
 * @version 1.0
 */
@SpringBootTest
@TestPropertySource(properties = "localmq.state-file=target/application-tests-state.json")
public class ApplicationTests {
    @Test
    public void contentLoad() {
    }
}
