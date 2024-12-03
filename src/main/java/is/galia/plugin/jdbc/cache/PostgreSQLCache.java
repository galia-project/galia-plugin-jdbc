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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.zaxxer.hikari.HikariDataSource;
import is.galia.async.VirtualThreadPool;
import is.galia.cache.AbstractCache;
import is.galia.stream.CompletableOutputStream;
import is.galia.cache.InfoCache;
import is.galia.cache.VariantCache;
import is.galia.config.Configuration;
import is.galia.image.Identifier;
import is.galia.image.Info;
import is.galia.image.StatResult;
import is.galia.operation.OperationList;
import is.galia.plugin.Plugin;
import is.galia.plugin.jdbc.config.Key;
import is.galia.plugin.jdbc.stream.LargeObjectInputStream;
import is.galia.plugin.jdbc.stream.LargeObjectOutputStream;
import is.galia.plugin.jdbc.utils.ConnectionUtils;
import is.galia.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static is.galia.config.Key.VARIANT_CACHE_TTL;

/**
 * <p>Cache using two tables, storing images as Large Objects and image infos
 * as native JSON.</p>
 *
 * <p>Requires the PostgreSQL "lo" extension.</p>
 *
 * <p>A database schema must be created manually&mdash;it will not be done
 * automatically. The current schema is:</p>
 *
 * <pre>{@code
 * CREATE EXTENSION lo;
 *
 * CREATE TABLE {PostgreSQLCache.variant_image_table} (
 *     operations VARCHAR(4096) NOT NULL,
 *     image_oid OID,
 *     last_modified TIMESTAMP,
 *     last_accessed TIMESTAMP
 * );
 *
 * CREATE UNIQUE INDEX variant_operations_idx ON {PostgreSQLCache.variant_image_table} (operations);
 * CREATE INDEX variant_last_modified_idx ON {PostgreSQLCache.variant_image_table} (last_modified);
 * CREATE INDEX variant_last_accessed_idx ON {PostgreSQLCache.variant_image_table} (last_accessed);
 *
 * CREATE TABLE {PostgreSQLCache.info_table} (
 *     identifier VARCHAR(4096) NOT NULL,
 *     info_json JSON NOT NULL,
 *     last_modified TIMESTAMP,
 *     last_accessed TIMESTAMP
 * );
 *
 * CREATE UNIQUE INDEX info_operations_idx ON {PostgreSQLCache.info_table} (identifier);
 * CREATE INDEX info_last_modified_idx ON {PostgreSQLCache.info_table} (last_modified);
 * CREATE INDEX info_last_accessed_idx ON {PostgreSQLCache.info_table} (last_accessed);
 *
 * CREATE TRIGGER t_galia_cache
 *     BEFORE UPDATE OR DELETE ON {PostgreSQLCache.variant_image_table}
 *     FOR EACH ROW EXECUTE FUNCTION lo_manage(image_oid);
 * }</pre>
 */
public final class PostgreSQLCache extends AbstractCache
        implements VariantCache, InfoCache, Plugin {

    private static final Logger LOGGER = LoggerFactory.
            getLogger(PostgreSQLCache.class);

    public static final String VARIANT_IMAGE_TABLE_IMAGE_OID_COLUMN     = "image_oid";
    public static final String VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN = "last_accessed";
    public static final String VARIANT_IMAGE_TABLE_LAST_MODIFIED_COLUMN = "last_modified";
    public static final String VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN    = "operations";

    public static final String INFO_TABLE_IDENTIFIER_COLUMN    = "identifier";
    public static final String INFO_TABLE_INFO_COLUMN          = "info_json";
    public static final String INFO_TABLE_LAST_ACCESSED_COLUMN = "last_accessed";
    public static final String INFO_TABLE_LAST_MODIFIED_COLUMN = "last_modified";

    private static final int DEFAULT_CONNECTION_TIMEOUT = 30;
    private static final int DEFAULT_MAX_POOL_SIZE =
            Runtime.getRuntime().availableProcessors() * 4;

    private static HikariDataSource dataSource;

    /**
     * @return Connection from the connection pool. Clients must call
     *         {@link Connection#close} when they are done with it.
     */
    public static synchronized Connection getConnection() throws SQLException {
        if (dataSource == null) {
            final Configuration config = Configuration.forApplication();
            final String jdbcURL       = config.getString(Key.POSTGRESQLCACHE_JDBC_URL.key(), "");
            final String user          = config.getString(Key.POSTGRESQLCACHE_USER.key(), "");
            final String password      = config.getString(Key.POSTGRESQLCACHE_PASSWORD.key(), "");
            // Useful for leak detection
            //System.setProperty("com.zaxxer.hikari.housekeeping.periodMs", "1000");
            dataSource = new HikariDataSource();
            dataSource.setJdbcUrl(jdbcURL);
            dataSource.setUsername(user);
            dataSource.setPassword(password);
            dataSource.setPoolName(PostgreSQLCache.class.getSimpleName() + "Pool");
            dataSource.setMaximumPoolSize(getMaxPoolSize());
            dataSource.setConnectionTimeout(getConnectionTimeout());
            //dataSource.setLeakDetectionThreshold(4000);

            // Create a connection in order to log some things and check
            // whether the database is sane.
            try (Connection connection = dataSource.getConnection()) {
                final DatabaseMetaData metadata = connection.getMetaData();
                LOGGER.debug("Using {} {}", metadata.getDriverName(),
                        metadata.getDriverVersion());
                LOGGER.debug("Connection URL: {}",
                        config.getString(Key.POSTGRESQLCACHE_JDBC_URL.key()));
                LOGGER.debug("Max connection pool size: {}", getMaxPoolSize());

                final String[] tableNames = { getVariantImageTableName(),
                        getInfoTableName() };
                for (String tableName : tableNames) {
                    try (ResultSet rs = metadata.getTables(null, null, tableName.toUpperCase(), null)) {
                        if (!rs.next()) {
                            LOGGER.error("Missing table: {}", tableName);
                        }
                    }
                }
            }
        }
        return dataSource.getConnection();
    }

    /**
     * @return Connection timeout in milliseconds.
     */
    private static int getConnectionTimeout() {
        return 1000 * Configuration.forApplication().getInt(
                Key.POSTGRESQLCACHE_CONNECTION_TIMEOUT.key(),
                DEFAULT_CONNECTION_TIMEOUT);
    }

    /**
     * @return Maximum connection pool size.
     */
    static int getMaxPoolSize() {
        return Configuration.forApplication().getInt(
                Key.POSTGRESQLCACHE_MAX_POOL_SIZE.key(),
                DEFAULT_MAX_POOL_SIZE);
    }

    /**
     * @return Name of the variant image table.
     * @throws IllegalArgumentException If the image table name is not set.
     */
    public static String getVariantImageTableName() {
        final String name = Configuration.forApplication().
                getString(Key.POSTGRESQLCACHE_VARIANT_IMAGE_TABLE.key(), "");
        if (name.isBlank()) {
            throw new IllegalArgumentException(
                    Key.POSTGRESQLCACHE_VARIANT_IMAGE_TABLE + " is not set");
        }
        return name;
    }

    /**
     * @return Name of the image info table.
     * @throws IllegalArgumentException If the info table name is not set.
     */
    public static String getInfoTableName() {
        final String name = Configuration.forApplication().
                getString(Key.POSTGRESQLCACHE_INFO_TABLE.key(), "");
        if (name.isBlank()) {
            throw new IllegalArgumentException(
                    Key.POSTGRESQLCACHE_INFO_TABLE + " is not set");
        }
        return name;
    }

    /**
     * Updates the last-accessed time of the variant image corresponding to
     * the given operation list.
     */
    private void touchVariantImage(OperationList opList,
                                   Connection connection) throws SQLException {
        final String sql = String.format(
                "UPDATE %s SET %s = ? WHERE %s = ?",
                getVariantImageTableName(),
                VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN,
                VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.setString(2, opList.toString());
            LOGGER.trace(sql);
            statement.executeUpdate();
        }
    }

    /**
     * Updates the last-accessed time of the variant image corresponding to the
     * given operation list asynchronously.
     */
    private void touchVariantImageAsync(OperationList opList) {
        VirtualThreadPool.getInstance().submit(() -> {
            try (Connection conn = getConnection()) {
                touchVariantImage(opList, conn);
            } catch (SQLException e) {
                LOGGER.warn("touchVariantImageAsync(): {}", e.getMessage());
            }
        });
    }

    /**
     * Updates the last-accessed time of the info corresponding to the given
     * identifier.
     */
    private void touchInfo(Identifier identifier,
                           Connection connection) throws SQLException {
        final String sql = String.format(
                "UPDATE %s SET %s = ? WHERE %s = ?",
                getInfoTableName(),
                INFO_TABLE_LAST_ACCESSED_COLUMN,
                INFO_TABLE_IDENTIFIER_COLUMN);
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setTimestamp(1, Timestamp.from(Instant.now()));
            statement.setString(2, identifier.toString());
            LOGGER.trace(sql);
            statement.executeUpdate();
        }
    }

    /**
     * Updates the last-accessed time of the info corresponding to the given
     * operation list asynchronously.
     */
    private void touchInfoAsync(Identifier identifier) {
        VirtualThreadPool.getInstance().submit(() -> {
            try (Connection conn = getConnection()) {
                touchInfo(identifier, conn);
            } catch (SQLException e) {
                LOGGER.error("touchInfoAsync(): {}", e.getMessage());
            }
        });
    }

    Timestamp getEarliestValidTimestamp() {
        final long ttl = Configuration.forApplication()
                .getLong(VARIANT_CACHE_TTL, 0);
        if (ttl > 0) {
            return new Timestamp(System.currentTimeMillis() - ttl * 1000);
        } else {
            return new Timestamp(0);
        }
    }

    /**
     * @param conn Will not be closed.
     * @return The number of purged infos.
     */
    private int purgeInfos(Connection conn) throws SQLException {
        final String sql = "DELETE FROM " + getInfoTableName();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            LOGGER.trace(sql);
            return statement.executeUpdate();
        }
    }

    /**
     * @param conn Will not be closed.
     * @return Number of images purged.
     */
    private int purgeExpiredVariantImages(Connection conn)
            throws SQLException {
        final String sql = String.format("DELETE FROM %s WHERE %s < ?",
                getVariantImageTableName(),
                VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setTimestamp(1, getEarliestValidTimestamp());
            LOGGER.trace(sql);
            return statement.executeUpdate();
        }
    }

    /**
     * @param conn Will not be closed.
     * @return Number of infos purged.
     */
    private int purgeExpiredInfos(Connection conn)
            throws SQLException {
        final String sql = String.format("DELETE FROM %s WHERE %s < ?",
                getInfoTableName(), INFO_TABLE_LAST_ACCESSED_COLUMN);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setTimestamp(1, getEarliestValidTimestamp());
            LOGGER.trace(sql);
            return statement.executeUpdate();
        }
    }

    /**
     * @param ops Operation list corresponding to the variant image to purge.
     * @param conn Will not be closed.
     * @return Number of purged images
     */
    private int purgeVariantImage(OperationList ops,
                                  Connection conn) throws SQLException {
        final String sql = String.format("DELETE FROM %s WHERE %s = ?",
                getVariantImageTableName(),
                VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, ops.toString());
            LOGGER.trace(sql);
            return statement.executeUpdate();
        }
    }

    /**
     * @param ops Operation list corresponding to the variant image to purge.
     */
    private void purgeVariantImageAsync(OperationList ops) {
        VirtualThreadPool.getInstance().submit(() -> {
            try (Connection conn = getConnection()) {
                purgeVariantImage(ops, conn);
            } catch (SQLException e) {
                LOGGER.error("purgeVariantImageAsync(): {}", e.getMessage());
            }
        });
    }

    /**
     * Purges all variant images.
     *
     * @param conn Will not be closed.
     * @return Number of purged images
     */
    private int purgeVariantImages(Connection conn) throws SQLException {
        final String sql = "DELETE FROM " + getVariantImageTableName();
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            LOGGER.trace(sql);
            return statement.executeUpdate();
        }
    }

    /**
     * Purges all variant images corresponding to the source image with the
     * given identifier.
     *
     * @param identifier
     * @param conn Will not be closed.
     * @return The number of purged images
     */
    private int purgeVariantImages(Identifier identifier,
                                   Connection conn) throws SQLException {
        final String sql = "DELETE FROM " + getVariantImageTableName() +
                " WHERE " + VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN +
                " LIKE ?";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, identifier.toString() + "%");
            LOGGER.trace(sql);
            return statement.executeUpdate();
        }
    }

    /**
     * Purges the info corresponding to the source image with the given
     * identifier.
     *
     * @param identifier
     * @param conn Will not be closed.
     * @return The number of purged infos.
     */
    private int purgeInfo(Identifier identifier,
                          Connection conn) throws SQLException {
        final String sql = String.format("DELETE FROM %s WHERE %s = ?",
                getInfoTableName(), INFO_TABLE_IDENTIFIER_COLUMN);
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.setString(1, identifier.toString());
            LOGGER.trace(sql);
            return statement.executeUpdate();
        }
    }

    private void purgeInfoAsync(Identifier identifier) {
        VirtualThreadPool.getInstance().submit(() -> {
            try (Connection conn = getConnection()) {
                purgeInfo(identifier, conn);
            } catch (SQLException e) {
                LOGGER.error("purgeInfoAsync(): {}", e.getMessage());
            }
        });
    }

    //endregion
    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Arrays.stream(Key.values())
                .map(Key::toString)
                .filter(k -> k.contains(PostgreSQLCache.class.getSimpleName()))
                .collect(Collectors.toSet());
    }

    @Override
    public String getPluginName() {
        return getClass().getSimpleName();
    }

    @Override
    public void onApplicationStart() {}

    @Override
    public void onApplicationStop() {}

    @Override
    public void initializePlugin() {}

    //endregion
    //region Cache methods

    @Override
    public void evict(Identifier identifier) throws IOException {
        try (Connection connection = getConnection()) {
            int numDeletedImages = purgeVariantImages(identifier, connection);
            int numDeletedInfos  = purgeInfo(identifier, connection);
            LOGGER.debug("Deleted {} cached image(s) and {} cached info(s)",
                    numDeletedImages, numDeletedInfos);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void evictInvalid() throws IOException {
        try (Connection connection = getConnection()) {
            int numDeletedVariantImages = purgeExpiredVariantImages(connection);
            int numDeletedInfos         = purgeExpiredInfos(connection);
            LOGGER.debug("evictInvalid(): purged {} variant images and {} info(s)",
                    numDeletedVariantImages, numDeletedInfos);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void purge() throws IOException {
        try (Connection connection = getConnection()) {
            int numDeletedVariantImages = purgeVariantImages(connection);
            int numDeletedInfos         = purgeInfos(connection);
            LOGGER.debug("Purged {} variant images and {} infos",
                    numDeletedVariantImages, numDeletedInfos);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    //endregion
    //region InfoCache methods

    @Override
    public void evictInfos() throws IOException {
        try (Connection connection = getConnection()) {
            int numDeleted;
            final String sql = "DELETE FROM " + getInfoTableName();
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                LOGGER.trace(sql);
                numDeleted = statement.executeUpdate();
            }
            LOGGER.debug("purgeInfos(): purged {} info(s)", numDeleted);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public Optional<Info> fetchInfo(Identifier identifier) throws IOException {
        final String sql = String.format(
                "SELECT %s FROM %s WHERE %s = ? AND %s >= ?",
                INFO_TABLE_INFO_COLUMN,
                getInfoTableName(),
                INFO_TABLE_IDENTIFIER_COLUMN,
                INFO_TABLE_LAST_ACCESSED_COLUMN);
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, identifier.toString());
            statement.setTimestamp(2, getEarliestValidTimestamp());
            LOGGER.trace(sql);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    touchInfoAsync(identifier);
                    LOGGER.trace("Hit for info: {}", identifier);
                    String json = resultSet.getString(1);
                    return Optional.of(Info.fromJSON(json));
                } else {
                    LOGGER.trace("Miss for info: {}", identifier);
                    purgeInfoAsync(identifier);
                }
            }
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
        return Optional.empty();
    }

    @Override
    public void put(Identifier identifier, Info info) throws IOException {
        try {
            put(identifier, info.toJSON());
        } catch (JsonProcessingException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void put(Identifier identifier, String info) throws IOException {
        LOGGER.debug("put(): {}", identifier);
        final String sql = String.format(
                "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
                getInfoTableName(),
                INFO_TABLE_IDENTIFIER_COLUMN,
                INFO_TABLE_INFO_COLUMN,
                INFO_TABLE_LAST_MODIFIED_COLUMN,
                INFO_TABLE_LAST_ACCESSED_COLUMN);
        Connection connection = null;
        try {
            connection = getConnection();
            connection.setAutoCommit(false);
            // Delete any existing info corresponding to the given identifier.
            purgeInfo(identifier, connection);

            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                // Add a new info corresponding to the given identifier.
                statement.setString(1, identifier.toString());
                statement.setString(2, info);
                Timestamp now = Timestamp.from(Instant.now());
                statement.setTimestamp(3, now);
                statement.setTimestamp(4, now);
                LOGGER.trace(sql);
                statement.executeUpdate();
            }
            connection.commit();
        } catch (SQLException e) {
            ConnectionUtils.rollbackQuietly(connection);
            throw new IOException(e.getMessage(), e);
        } finally {
            IOUtils.closeQuietly(connection);
        }
    }

    //endregion
    //region VariantCache methods

    @Override
    public void evict(OperationList ops) throws IOException {
        try (Connection connection = getConnection()) {
            int numDeletedImages = purgeVariantImage(ops, connection);
            LOGGER.debug("Purged {} variant images", numDeletedImages);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public InputStream newVariantImageInputStream(
            OperationList opList,
            StatResult statResult) throws IOException {
        final String sql = String.format(
                "SELECT %s, %s FROM %s WHERE %s = ? AND %s >= ?",
                VARIANT_IMAGE_TABLE_IMAGE_OID_COLUMN,
                VARIANT_IMAGE_TABLE_LAST_MODIFIED_COLUMN,
                getVariantImageTableName(),
                VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN,
                VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN);
        try (Connection connection = getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, opList.toString());
            statement.setTimestamp(2, getEarliestValidTimestamp());
            LOGGER.trace(sql);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    LOGGER.debug("Hit for image: {}", opList);
                    long oid = resultSet.getLong(1);
                    statResult.setLastModified(resultSet.getTimestamp(2).toInstant());
                    touchVariantImageAsync(opList);
                    return new LargeObjectInputStream(oid);
                } else {
                    LOGGER.debug("Miss for image: {}", opList);
                    purgeVariantImageAsync(opList);
                }
            }
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
        return null;
    }

    @Override
    public CompletableOutputStream
    newVariantImageOutputStream(OperationList ops) throws IOException {
        LOGGER.debug("Miss; caching {}", ops);
        try {
            return new LargeObjectOutputStream(ops, getAllObservers());
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    //endregion

}
