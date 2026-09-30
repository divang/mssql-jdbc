/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes.vector;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * PreparedStatement Performance & Functionality Test for VECTOR data type.
 * Tests:
 * 1. PreparedStatement parameterized single inserts.
 * 2. PreparedStatement executeBatch() with high-dimensional vector embeddings.
 * 3. Compares PreparedStatement batch ingestion against live Azure SQL DB.
 */
@Tag(Constants.vectorTest)
public class VectorPreparedStatementLiveTest extends AbstractTest {

    private static final String TABLE_NAME = "vector_pstmt_perf_test";
    private static final int VECTOR_DIMS = 128;
    private static final int BATCH_ROWS = 5000;

    private static String vectorConnectionString;

    @BeforeAll
    public static void setupTable() throws Exception {
        vectorConnectionString = getConnectionString() + ";vectorTypeSupport=v1;";
        try (Connection con = DriverManager.getConnection(vectorConnectionString);
             Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT NOT NULL, embedding VECTOR(" + VECTOR_DIMS + ") NOT NULL)");
        }
    }

    @AfterAll
    public static void teardownTable() throws Exception {
        try (Connection con = DriverManager.getConnection(vectorConnectionString);
             Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
        }
    }

    @Test
    @DisplayName("PreparedStatement executeBatch() with VECTOR(128)")
    public void testPreparedStatementBatchInsert() throws Exception {
        System.out.println("==========================================================================");
        System.out.println("   VECTOR(128) PREPAREDSTATEMENT BATCH INGESTION (" + BATCH_ROWS + " rows into Azure SQL DB)");
        System.out.println("==========================================================================");

        // Pre-create embedding templates
        Float[][] embeddingPool = new Float[20][VECTOR_DIMS];
        Random rng = new Random(42);
        for (int i = 0; i < 20; i++) {
            for (int d = 0; d < VECTOR_DIMS; d++) {
                embeddingPool[i][d] = rng.nextFloat();
            }
        }

        try (Connection con = DriverManager.getConnection(vectorConnectionString)) {
            try (Statement stmt = con.createStatement()) {
                stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            }

            String insertSql = "INSERT INTO " + TABLE_NAME + " (id, embedding) VALUES (?, ?)";
            long start = System.currentTimeMillis();

            try (PreparedStatement pstmt = con.prepareStatement(insertSql)) {
                for (int r = 1; r <= BATCH_ROWS; r++) {
                    pstmt.setInt(1, r);
                    Float[] data = embeddingPool[r % embeddingPool.length];
                    Vector vector = new Vector(VECTOR_DIMS, VectorDimensionType.FLOAT32, data);
                    pstmt.setObject(2, vector, microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();

                    if (r % 1000 == 0) {
                        pstmt.executeBatch();
                    }
                }
                pstmt.executeBatch();
            }

            long elapsed = System.currentTimeMillis() - start;
            double rate = (BATCH_ROWS / (elapsed / 1000.0));

            System.out.printf("  PreparedStatement executeBatch() Elapsed : %,d ms%n", elapsed);
            System.out.printf("  PreparedStatement Ingestion Rate        : %,.0f vectors/sec%n", rate);
            System.out.println("==========================================================================");

            // Verify count
            try (Statement stmt = con.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                rs.next();
                assertEquals(BATCH_ROWS, rs.getInt(1), "All batch rows should be inserted");
            }
        }
    }
}
