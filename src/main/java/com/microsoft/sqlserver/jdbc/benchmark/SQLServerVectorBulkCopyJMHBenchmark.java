/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.benchmark;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Publication-Grade JMH Benchmark for Vector (FLOAT32) Data Type.
 * Measures:
 * 1. Throughput (operations / second of 5,000 VECTOR(128) rows)
 * 2. Heap Allocation Rate via JMH GCProfiler (B/op and MB/sec)
 * 3. Churn differences between Standard On-Heap ByteBuffer and MemorySegment Off-Heap
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 3, time = 4, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Xms1g", "-Xmx1g", "-XX:+UseG1GC", "--enable-native-access=ALL-UNNAMED"})
public class SQLServerVectorBulkCopyJMHBenchmark {

    private static final String TABLE_NAME = "jmh_vector_benchmark";
    private static final int VECTOR_DIMS = 128;
    private static final int ROWS_PER_OPERATION = 5_000;
    private static final int BATCH_SIZE = 2_500;

    @Param({"false", "true"})
    public boolean useMemorySegment;

    private String jdbcUrl;
    private String user;
    private String password;
    private Connection connection;
    private Float[][] embeddingPool;

    private Connection createConnection() throws SQLException {
        if (jdbcUrl.toLowerCase().contains("user=") || jdbcUrl.toLowerCase().contains("username=")) {
            return DriverManager.getConnection(jdbcUrl);
        } else {
            return DriverManager.getConnection(jdbcUrl, user, password);
        }
    }

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        String rawConn = System.getProperty("jmh.jdbcUrl");
        if (rawConn == null || rawConn.isEmpty()) {
            rawConn = System.getenv("MSSQL_TEST_URL");
        }
        if (rawConn == null || rawConn.isEmpty()) {
            rawConn = System.getenv("mssql_jdbc_test_connection_properties");
        }
        if (rawConn == null || rawConn.isEmpty()) {
            rawConn = System.getenv("MSSQL_JDBC_TEST_CONNECTION_PROPERTIES");
        }
        if (rawConn == null || rawConn.isEmpty()) {
            rawConn = "jdbc:sqlserver://divang-personal.database.windows.net:1433;databaseName=test-divang-driver;user=divang-test@divang-personal;password=Driver!@#;encrypt=true;trustServerCertificate=false;loginTimeout=60;";
        }
        if (!rawConn.contains("vectorTypeSupport")) {
            rawConn += ";vectorTypeSupport=v1;";
        }
        jdbcUrl = rawConn;

        user = System.getProperty("jmh.user",
                System.getenv().getOrDefault("MSSQL_TEST_USER", "sa"));
        password = System.getProperty("jmh.password",
                System.getenv().getOrDefault("MSSQL_TEST_PASS", "Local_356f69c891874a78425f82a2_Pw1!"));

        // Pre-create embedding templates to avoid benchmark GC noise
        embeddingPool = new Float[50][VECTOR_DIMS];
        Random rng = new Random(42);
        for (int i = 0; i < 50; i++) {
            for (int d = 0; d < VECTOR_DIMS; d++) {
                embeddingPool[i][d] = rng.nextFloat();
            }
        }

        // Retry connection logic for cloud serverless database wakeups
        SQLException lastEx = null;
        for (int attempt = 1; attempt <= 5; attempt++) {
            try (Connection testCon = createConnection()) {
                lastEx = null;
                break;
            } catch (SQLException ex) {
                lastEx = ex;
                Thread.sleep(2000);
            }
        }
        if (lastEx != null) {
            throw lastEx;
        }

        try (Connection con = createConnection();
             Statement stmt = con.createStatement()) {
            stmt.execute("IF OBJECT_ID('" + TABLE_NAME + "', 'U') IS NOT NULL DROP TABLE " + TABLE_NAME);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
                    + "id INT NOT NULL, "
                    + "embedding VECTOR(" + VECTOR_DIMS + ") NOT NULL"
                    + ")");
        }

        connection = createConnection();
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("IF OBJECT_ID('" + TABLE_NAME + "', 'U') IS NOT NULL DROP TABLE " + TABLE_NAME);
            }
            connection.close();
        }
    }

    @Setup(Level.Iteration)
    public void setupIteration() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
        }
    }

    @Benchmark
    public void vectorBulkInsert(Blackhole bh) throws Exception {
        JMHVectorRowSource source = new JMHVectorRowSource(ROWS_PER_OPERATION, VECTOR_DIMS, embeddingPool);
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setBatchSize(BATCH_SIZE);
            options.setBulkCopyTimeout(180);
            options.setUseMemorySegment(useMemorySegment);
            bulkCopy.setBulkCopyOptions(options);
            bulkCopy.setDestinationTableName(TABLE_NAME);
            bulkCopy.writeToServer(source);
        }
        bh.consume(source);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(SQLServerVectorBulkCopyJMHBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .build();

        new Runner(opt).run();
    }

    private static final class JMHVectorRowSource implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private final int dimensions;
        private final Float[][] pool;
        private int currentRow = 0;
        private final Object[] preallocatedRow = new Object[2];

        JMHVectorRowSource(int totalRows, int dimensions, Float[][] pool) {
            this.totalRows = totalRows;
            this.dimensions = dimensions;
            this.pool = pool;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1, 2));
        }

        @Override
        public String getColumnName(int column) {
            return column == 1 ? "id" : "embedding";
        }

        @Override
        public int getColumnType(int column) {
            return column == 1 ? java.sql.Types.INTEGER : microsoft.sql.Types.VECTOR;
        }

        @Override
        public int getPrecision(int column) {
            return column == 2 ? dimensions : 0;
        }

        @Override
        public int getScale(int column) {
            return 0; // FLOAT32 scale = 0
        }

        @Override
        public Object[] getRowData() {
            preallocatedRow[0] = currentRow;
            Float[] vectorData = pool[currentRow % pool.length];
            preallocatedRow[1] = new Vector(dimensions, VectorDimensionType.FLOAT32, vectorData);
            return preallocatedRow;
        }

        @Override
        public boolean next() {
            if (currentRow < totalRows) {
                currentRow++;
                return true;
            }
            return false;
        }
    }
}
