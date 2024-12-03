/*
 * Copyright © 2024 Baird Creek Software LLC
 *
 * Licensed under the PolyForm Noncommercial License, version 1.0.0;
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://polyformproject.org/licenses/noncommercial/1.0.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package is.galia.plugin.jdbc.cache;

import is.galia.cache.CacheObserver;
import is.galia.cache.InfoCache;
import is.galia.config.Configuration;
import is.galia.image.Format;
import is.galia.image.Info;
import is.galia.image.Identifier;
import is.galia.image.StatResult;
import is.galia.operation.Encode;
import is.galia.operation.OperationList;
import is.galia.plugin.jdbc.BaseTest;
import is.galia.plugin.jdbc.config.Key;
import is.galia.plugin.jdbc.test.PostgreSQLCacheUtils;
import is.galia.stream.CompletableOutputStream;
import is.galia.util.ConcurrentProducerConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static is.galia.config.Key.VARIANT_CACHE_TTL;
import static is.galia.plugin.jdbc.test.TestUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.*;

class PostgreSQLCacheTest extends BaseTest {

    static final int ASYNC_WAIT = 3000;

    private PostgreSQLCache instance;

    @BeforeAll
    public static void beforeClass() {
        new PostgreSQLCache().onApplicationStart();
    }

    @BeforeEach
    @Override
    public void setUp() throws Exception {
        super.setUp();

        Configuration config = Configuration.forApplication();
        assumeFalse(config.getString(Key.POSTGRESQLCACHE_JDBC_URL.key(), "").isBlank());

        config.setProperty(VARIANT_CACHE_TTL, 300);

        PostgreSQLCacheUtils.dropCacheTables();
        PostgreSQLCacheUtils.createCacheTables();

        instance = new PostgreSQLCache();
        instance.initializePlugin();
        instance.purge();
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        PostgreSQLCacheUtils.dropCacheTables();
    }

    //region Plugin methods

    @Test
    void getPluginConfigKeys() {
        Set<String> keys = instance.getPluginConfigKeys();
        assertFalse(keys.isEmpty());
    }

    @Test
    void getPluginName() {
        assertEquals(PostgreSQLCache.class.getSimpleName(),
                instance.getPluginName());
    }

    //endregion
    //region Cache methods

    /* earliestValidTimestamp() */

    @Test
    void earliestValidTimestamp() {
        final Configuration config = Configuration.forApplication();

        // ttl = 0
        config.setProperty(VARIANT_CACHE_TTL, 0);
        assertEquals(new Timestamp(0), instance.getEarliestValidTimestamp());

        // ttl = 50
        config.setProperty(VARIANT_CACHE_TTL, 50);
        Instant expected = Instant.now().minus(Duration.ofSeconds(50)).
                truncatedTo(ChronoUnit.SECONDS);
        Instant actual = instance.getEarliestValidTimestamp().toInstant().
                truncatedTo(ChronoUnit.SECONDS);
        assertEquals(expected, actual);
    }

    /* evict(Identifier) */

    @Test
    void evict() throws Exception {
        // add an image and an info
        final Identifier id1        = new Identifier("cats");
        final OperationList opList1 = OperationList.builder()
                .withIdentifier(id1)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList1)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }
        instance.put(id1, new Info());

        // add another image and another info
        final Identifier id2        = new Identifier("dogs");
        final OperationList opList2 = OperationList.builder()
                .withIdentifier(id2)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList2)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }
        instance.put(id2, new Info());

        assertNotNull(instance.fetchInfo(id1));
        assertNotNull(instance.fetchInfo(id2));

        // evict one of the info/image pairs
        instance.evict(id1);

        Thread.sleep(2000); // TODO: use a CacheObserver

        // assert that its info and image are gone
        assertFalse(instance.fetchInfo(id1).isPresent());

        try (InputStream is = instance.newVariantImageInputStream(opList1)) {
            assertNull(is);
        }

        // ... but the other one is still there
        assertNotNull(instance.fetchInfo(id2));
        try (InputStream is = instance.newVariantImageInputStream(opList2)) {
            assertNotNull(is);
        }
    }

    /* evict(OperationList) */

    @Test
    void evictWithOperationList() throws Exception {
        // Seed a variant image
        OperationList ops1 = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(ops1)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // Seed another variant image
        OperationList ops2 = OperationList.builder()
                .withIdentifier(new Identifier("dogs"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(ops2)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        Thread.sleep(ASYNC_WAIT);

        // Evict the first one
        instance.evict(ops1);

        // Assert that it's gone
        assertNotExists(instance, ops1);

        // Assert that the other one was NOT evicted
        assertExists(instance, ops2);
    }

    /* evictInfos() */

    @Test
    void evictInfos() throws Exception {
        Identifier identifier = new Identifier("cats");
        OperationList opList  = OperationList.builder()
                .withIdentifier(identifier)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info = new Info();

        // assert that a particular image doesn't exist
        try (InputStream is = instance.newVariantImageInputStream(opList)) {
            assertNull(is);
        }

        // assert that a particular info doesn't exist
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // add the image
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add the info
        instance.put(identifier, info);

        Thread.sleep(ASYNC_WAIT);

        // assert that they've been added
        assertExists(instance, opList);
        assertNotNull(instance.fetchInfo(identifier));

        // Evict infos
        instance.evictInfos();

        // assert that the info has been evicted
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // assert that the image has NOT been evicted
        assertExists(instance, opList);
    }

    /* evictInvalid() */

    @Test
    void evictInvalid() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 2);

        // add an image
        Identifier id1     = new Identifier("id1");
        OperationList ops1 = OperationList.builder()
                .withIdentifier(id1)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info1         = new Info();

        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops1)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add an Info
        instance.put(id1, info1);

        // wait for them to invalidate
        Thread.sleep(2100);

        // add another image
        Identifier id2     = new Identifier("cats");
        OperationList ops2 = OperationList.builder()
                .withIdentifier(new Identifier("id2"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info2         = new Info();

        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops2)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add another info
        instance.put(id2, info2);

        instance.evictInvalid();

        // assert that one image and one info have been evicted
        assertFalse(instance.fetchInfo(id1).isPresent());
        assertTrue(instance.fetchInfo(id2).isPresent());
        assertNotExists(instance, ops1);
        assertExists(instance, ops2);
    }

    /* fetchInfo(Identifier) */

    @Test
    void fetchInfoWithExistingValidImage() throws Exception {
        Identifier identifier = new Identifier("cats");
        Info info = new Info();
        instance.put(identifier, info);

        Optional<Info> actual = instance.fetchInfo(identifier);
        assertEquals(actual.orElseThrow(), info);
    }

    @Test
    void fetchInfoWithExistingInvalidImage() throws Exception {
        Configuration.forApplication().setProperty(is.galia.config.Key.VARIANT_CACHE_TTL, 1);

        Identifier identifier = new Identifier("cats");
        Info info             = new Info();
        instance.put(identifier, info);

        Thread.sleep(ASYNC_WAIT);

        assertFalse(instance.fetchInfo(identifier).isPresent());
    }

    @Test
    void fetchInfoWithNonexistentImage() throws Exception {
        assertFalse(instance.fetchInfo(new Identifier("bogus")).isPresent());
    }

    @Test
    void fetchInfoPopulatesSerializationTimestampWhenNotAlreadySet()
            throws Exception {
        Identifier identifier = new Identifier("cats");
        Info info             = new Info();
        instance.put(identifier, info);

        info = instance.fetchInfo(identifier).orElseThrow();
        assertNotNull(info.getSerializationTimestamp());
    }

    @Test
    void fetchInfoConcurrently() {
        // This is tested by putConcurrently()
    }

    @Test
    void fetchInfoUpdatesLastAccessedTime() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final Info info = Info.builder().withIdentifier(identifier).build();
        instance.put(identifier, info);

        // get the initial last-accessed time
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?;",
                PostgreSQLCache.INFO_TABLE_LAST_ACCESSED_COLUMN,
                PostgreSQLCache.getInfoTableName(),
                PostgreSQLCache.INFO_TABLE_IDENTIFIER_COLUMN);
        try (Connection connection = PostgreSQLCache.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identifier.toString());

            Timestamp time1, time2;
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                time1 = resultSet.getTimestamp(1);
            }
            // run the clock
            Thread.sleep(10);

            // this should cause the last-accessed time to update asynchronously
            instance.fetchInfo(identifier);

            Thread.sleep(100);

            // get the new last-accessed time
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                time2 = resultSet.getTimestamp(1);
            }
            // compare them
            assertTrue(time2.after(time1));
        }
    }

    /* newVariantImageInputStream(OperationList) */

    @Test
    void newVariantImageInputStreamWithZeroTTL() throws Exception {
        Configuration.forApplication().setProperty(is.galia.config.Key.VARIANT_CACHE_TTL, 0);

        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Write an image to the cache
        OperationList opList = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in and assert same size
        try (InputStream is = instance.newVariantImageInputStream(opList)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            is.transferTo(os);
            os.close();
            assertEquals(Files.size(FIXTURE), os.toByteArray().length);
        }
    }

    @Test
    void newVariantImageInputStreamWithNonzeroTTL() throws Exception {
        Configuration.forApplication().setProperty(is.galia.config.Key.VARIANT_CACHE_TTL, 2);

        OperationList opList   = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch   = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image. (The write may complete before data is fully or even
        // partially written to the cache.)
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Assert that it has been added.
        assertExists(instance, opList);
        // Wait for it to invalidate.
        Thread.sleep(3000);
        // Assert that it has been evicted.
        assertNotExists(instance, opList);
    }

    @Test
    void newVariantImageInputStreamWithNonexistentImage() {
        final OperationList ops = new OperationList(new Identifier("does not exist"));
        assertNotExists(instance, ops);
    }

    @Test
    void newVariantImageInputStreamConcurrently() throws Exception {
        final OperationList ops = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();

        new ConcurrentProducerConsumer(() -> {
            try (CompletableOutputStream os =
                         instance.newVariantImageOutputStream(ops)) {
                Files.copy(FIXTURE, os);
                os.complete();
            }
            return null;
        }, () -> {
            try (InputStream is = instance.newVariantImageInputStream(ops)) {
                if (is != null) {
                    //noinspection StatementWithEmptyBody
                    while (is.read() != -1) {
                        // consume the stream fully
                    }
                }
            }
            return null;
        }, PostgreSQLCache.getMaxPoolSize() - 5).run();
    }

    @Test
    void newVariantImageInputStreamUpdatesLastAccessedTime() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final OperationList opList  = OperationList.builder()
                .withIdentifier(identifier).build();

        final CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });
        try (CompletableOutputStream os = instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }
        latch.await(10, TimeUnit.SECONDS);

        // get the initial last-accessed time
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?;",
                PostgreSQLCache.VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN,
                PostgreSQLCache.getVariantImageTableName(),
                PostgreSQLCache.VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN);
        try (Connection connection = PostgreSQLCache.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, opList.toString());

            Timestamp time1, time2;
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                time1 = resultSet.getTimestamp(1);
            }
            // run the clock
            Thread.sleep(10);

            // Access the image to update the last-accessed time (this will
            // happen asynchronously)
            instance.newVariantImageInputStream(opList).close();

            // wait for it to happen
            Thread.sleep(100);

            // get the new last-accessed time
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                time2 = resultSet.getTimestamp(1);
            }
            // compare them
            assertTrue(time2.after(time1));
        }
    }

    /* newVariantImageInputStream(OperationList, StatResult) */

    @Test
    void newVariantImageInputStreamPopulatesStatResult() throws Exception {
        Configuration.forApplication().setProperty(VARIANT_CACHE_TTL, 0);
        OperationList opList = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Write an image to the cache
        try (CompletableOutputStream os =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, os);
            os.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in
        StatResult statResult = new StatResult();
        try (InputStream is = instance.newVariantImageInputStream(opList, statResult)) {
            assertNotNull(statResult.getLastModified());
            is.readAllBytes();
        }
    }

    /* newVariantImageOutputStream() */

    @Test
    void newVariantImageOutputStream() throws Exception {
        OperationList ops = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image to the cache
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Read it back in
        try (InputStream is = instance.newVariantImageInputStream(ops)) {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            is.transferTo(os);
            os.close();
            assertEquals(Files.size(FIXTURE), os.toByteArray().length);
        }
    }

    @Test
    void newVariantImageOutputStreamDoesNotLeaveDetritusWhenStreamIsIncompletelyWritten()
            throws Exception {
        OperationList ops    = OperationList.builder()
                .withIdentifier(new Identifier("cats"))
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        CountDownLatch latch = new CountDownLatch(1);
        instance.addObserver(new CacheObserver() {
            @Override
            public void onImageWritten(OperationList opList) {
                latch.countDown();
            }
        });

        // Add an image to the cache
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(ops)) {
            Files.copy(FIXTURE, outputStream);
            // don't set it complete
        }

        // (jump to onImageWritten())
        latch.await(10, TimeUnit.SECONDS);

        // Try to read it back in
        try (InputStream is = instance.newVariantImageInputStream(ops)) {
            assertNull(is);
        }
    }

    @Test
    void newVariantImageOutputStreamConcurrently() {
        // This is tested in testNewVariantImageInputStreamConcurrently()
    }

    @Test
    void newVariantImageOutputStreamOverwritesExistingImage() {
        // TODO: write this
    }

    /* purge() */

    @Test
    void purge() throws Exception {
        Identifier identifier = new Identifier("cats");
        OperationList opList = OperationList.builder()
                .withIdentifier(identifier)
                .withOperations(new Encode(Format.get("jpg")))
                .build();
        Info info = new Info();

        // assert that a particular image doesn't exist
        try (InputStream is = instance.newVariantImageInputStream(opList)) {
            assertNull(is);
        }

        // assert that a particular info doesn't exist
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // add the image
        try (CompletableOutputStream outputStream =
                     instance.newVariantImageOutputStream(opList)) {
            Files.copy(FIXTURE, outputStream);
            outputStream.complete();
        }

        // add the info
        instance.put(identifier, info);

        Thread.sleep(ASYNC_WAIT);

        // assert that they've been added
        assertExists(instance, opList);
        assertNotNull(instance.fetchInfo(identifier));

        // purge everything
        instance.purge();

        // assert that the info has been evicted
        assertFalse(instance.fetchInfo(identifier).isPresent());

        // assert that the image has been evicted
        assertNotExists(instance, opList);
    }

    /* put(Identifier, Info) */

    @Test
    void putWithInfo() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final Info info             = new Info();

        instance.put(identifier, info);

        Optional<Info> actualInfo = instance.fetchInfo(identifier);
        assertEquals(info, actualInfo.orElseThrow());
    }

    /**
     * Tests that concurrent calls of {@link
     * InfoCache#put(Identifier, Info)} and {@link
     * InfoCache#fetchInfo(Identifier)} don't conflict.
     */
    @Test
    void putWithInfoConcurrently() throws Exception {
        final Identifier identifier = new Identifier("monkeys");
        final Info info             = new Info();

        new ConcurrentProducerConsumer(() -> {
            instance.put(identifier, info);
            return null;
        }, () -> {
            Optional<Info> otherInfo = instance.fetchInfo(identifier);
            if (otherInfo.isPresent() && !info.equals(otherInfo.get())) {
                fail();
            }
            return null;
        }, PostgreSQLCache.getMaxPoolSize() - 5).run();
    }

    @Test
    void putWithInfoSetsLastAccessedTime() throws Exception {
        Identifier identifier = new Identifier("birds");
        Info info = new Info();
        instance.put(identifier, info);

        // get the initial last-accessed time
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?;",
                PostgreSQLCache.INFO_TABLE_LAST_ACCESSED_COLUMN,
                PostgreSQLCache.getInfoTableName(),
                PostgreSQLCache.INFO_TABLE_IDENTIFIER_COLUMN);
        try (Connection connection = PostgreSQLCache.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identifier.toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                assertNotNull(resultSet.getTimestamp(1));
            }
        }
    }

    /* put(Identifier, String) */

    @Test
    void putWithString() throws Exception {
        final Identifier identifier = new Identifier("cats");
        final Info info             = new Info();
        final String infoStr        = info.toJSON();

        instance.put(identifier, infoStr);

        Optional<Info> actualInfo = instance.fetchInfo(identifier);
        assertEquals(info, actualInfo.orElseThrow());
    }

    /**
     * Tests that concurrent calls of {@link
     * InfoCache#put(Identifier, String)} and {@link
     * InfoCache#fetchInfo(Identifier)} don't conflict.
     */
    @Test
    void putWithStringConcurrently() throws Exception {
        final Identifier identifier = new Identifier("monkeys");
        final Info info             = new Info();
        final String infoStr        = info.toJSON();

        new ConcurrentProducerConsumer(() -> {
            instance.put(identifier, infoStr);
            return null;
        }, () -> {
            Optional<Info> otherInfo = instance.fetchInfo(identifier);
            if (otherInfo.isPresent() && !info.equals(otherInfo.get())) {
                fail();
            }
            return null;
        }, PostgreSQLCache.getMaxPoolSize() - 5).run();
    }

    @Test
    void putWithStringSetsLastAccessedTime() throws Exception {
        Identifier identifier = new Identifier("birds");
        Info info             = new Info();
        String infoStr        = info.toJSON();
        instance.put(identifier, infoStr);

        // get the initial last-accessed time
        String sql = String.format("SELECT %s FROM %s WHERE %s = ?;",
                PostgreSQLCache.INFO_TABLE_LAST_ACCESSED_COLUMN,
                PostgreSQLCache.getInfoTableName(),
                PostgreSQLCache.INFO_TABLE_IDENTIFIER_COLUMN);
        try (Connection connection = PostgreSQLCache.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identifier.toString());
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                assertNotNull(resultSet.getTimestamp(1));
            }
        }
    }

}
