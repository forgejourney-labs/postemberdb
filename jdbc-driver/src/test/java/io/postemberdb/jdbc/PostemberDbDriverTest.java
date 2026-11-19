package io.postemberdb.jdbc;

import org.junit.jupiter.api.Test;
import java.sql.Driver;
import java.util.Properties;
import java.util.TreeMap;
import static org.junit.jupiter.api.Assertions.*;

class PostemberDbDriverTest {
    @Test void acceptsOnlyPostemberDbUrls() throws Exception {
        Driver driver = new PostemberDbDriver();
        assertTrue(driver.acceptsURL("jdbc:postemberdb://127.0.0.1:7432"));
        assertFalse(driver.acceptsURL("jdbc:postgresql://127.0.0.1/test"));
    }

    @Test void preparedParametersAreQuotedAndQuestionMarksInsideLiteralsAreIgnored() throws Exception {
        TreeMap<Integer,Object> values = new TreeMap<>();
        values.put(1, "O'Brien");
        values.put(2, 42);
        String sql = PostemberDbDriver.bindSql("INSERT INTO t VALUES ('literal?', ?, ?)", values);
        assertEquals("INSERT INTO t VALUES ('literal?', 'O''Brien', 42)", sql);
    }

    @Test void missingPreparedParameterIsRejected() {
        assertThrows(java.sql.SQLException.class,
            () -> PostemberDbDriver.bindSql("SELECT ?", new TreeMap<>()));
    }
}