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

package is.galia.plugin.jdbc.source;

import com.zaxxer.hikari.HikariDataSource;
import is.galia.codec.FormatDetector;
import is.galia.config.Configuration;
import is.galia.delegate.DelegateException;
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.MediaType;
import is.galia.image.StatResult;
import is.galia.plugin.Plugin;
import is.galia.plugin.jdbc.config.Key;
import is.galia.source.AbstractSource;
import is.galia.source.FormatChecker;
import is.galia.source.IdentifierFormatChecker;
import is.galia.source.Source;
import is.galia.stream.ClosingMemoryCacheImageInputStream;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static is.galia.plugin.jdbc.utils.ConnectionUtils.rollbackQuietly;

/**
 * <p>Maps an identifier to a Large Object column in a relational database.</p>
 *
 * <p>A custom schema is not required; most schemas will work. However, several
 * delegate methods must be implemented in order to obtain the information
 * needed to run the SQL queries.</p>
 *
 * <h2>Format Inference</h2>
 *
 * <p>See {@link FormatIterator}.</p>
 */
public final class PostgreSQLSource extends AbstractSource
        implements Source, Plugin {

    /**
     * <ol>
     *     <li>If the {@link #MEDIA_TYPE_DELEGATE_METHOD} method returns either
     *     a media type, or a query that can be invoked to obtain one, and that
     *     is successful, that format will be used.</li>
     *     <li>If the source image's identifier has a recognized filename
     *     extension, the format will be inferred from that.</li>
     *     <li>Otherwise, a small range of data will be read from the beginning
     *     of the resource, and an attempt will be made to infer a format from
     *     any "magic bytes" it may contain.</li>
     * </ol>
     *
     * @param <T> {@link Format}.
     */
    public class FormatIterator<T> implements Iterator<T> {

        /**
         * Infers a {@link Format} based on the media type column.
         */
        private class MediaTypeColumnChecker implements FormatChecker {
            @Override
            public Format check() throws IOException {
                try {
                    String methodResult = getMediaType();
                    if (methodResult != null) {
                        // the delegate method result may be a media type, or an
                        // SQL statement to look it up.
                        if (methodResult.toUpperCase().startsWith("SELECT")) {
                            LOGGER.debug(methodResult);
                            try (Connection connection = getConnection();
                                 PreparedStatement statement = connection.
                                         prepareStatement(methodResult)) {
                                statement.setString(1, getDatabaseIdentifier());
                                try (ResultSet resultSet = statement.executeQuery()) {
                                    if (resultSet.next()) {
                                        String value = resultSet.getString(1);
                                        if (value != null) {
                                            return MediaType.fromString(value).toFormat();
                                        }
                                    }
                                }
                            }
                        } else {
                            return MediaType.fromString(methodResult).toFormat();
                        }
                    }
                } catch (SQLException | DelegateException e) {
                    throw new IOException(e);
                }
                return Format.UNKNOWN;
            }
        }

        /**
         * Infers a {@link Format} based on image magic bytes.
         */
        private class ByteChecker implements FormatChecker {
            @Override
            public Format check() throws IOException {
                Connection connection = null;
                try {
                    connection = getConnection();
                    connection.setAutoCommit(false);
                    final String sql = getLookupSQL();
                    try (PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, getDatabaseIdentifier());
                        LOGGER.trace(sql);
                        try (ResultSet result = statement.executeQuery()) {
                            if (result.next()) {
                                long oid = result.getLong(1);
                                LargeObjectManager manager =
                                        connection.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
                                try (LargeObject object = manager.open(oid, LargeObjectManager.READ);
                                     ImageInputStream is = new ClosingMemoryCacheImageInputStream(
                                             object.getInputStream())) {
                                    return FormatDetector.detect(is);
                                }
                            }
                        }
                    }
                } catch (DelegateException | SQLException e) {
                    throw new IOException(e);
                } finally {
                    rollbackQuietly(connection);
                }
                return Format.UNKNOWN;
            }
        }

        private FormatChecker formatChecker;

        @Override
        public boolean hasNext() {
            return (formatChecker == null ||
                    formatChecker instanceof IdentifierFormatChecker ||
                    formatChecker instanceof FormatIterator.MediaTypeColumnChecker);
        }

        @Override
        public T next() {
            if (formatChecker == null) {
                formatChecker = new IdentifierFormatChecker(identifier);
            } else if (formatChecker instanceof IdentifierFormatChecker) {
                formatChecker = new MediaTypeColumnChecker();
            } else if (formatChecker instanceof FormatIterator.MediaTypeColumnChecker) {
                formatChecker = new ByteChecker();
            } else {
                throw new NoSuchElementException();
            }
            try {
                //noinspection unchecked
                return (T) formatChecker.check();
            } catch (IOException e) {
                LOGGER.warn("Error checking format: {}", e.getMessage());
                //noinspection unchecked
                return (T) Format.UNKNOWN;
            }
        }
    }

    private static final Logger LOGGER =
            LoggerFactory.getLogger(PostgreSQLSource.class);

    private static final String DATABASE_IDENTIFIER_DELEGATE_METHOD =
            "postgresqlsource_database_identifier";
    private static final String MEDIA_TYPE_DELEGATE_METHOD =
            "postgresqlsource_media_type";
    private static final String LAST_MODIFIED_DELEGATE_METHOD =
            "postgresqlsource_last_modified";
    private static final String LOOKUP_SQL_DELEGATE_METHOD =
            "postgresqlsource_lookup_sql";

    private static final int DEFAULT_CONNECTION_TIMEOUT = 30;
    private static final int DEFAULT_MAX_POOL_SIZE =
            Runtime.getRuntime().availableProcessors() * 4;

    private static HikariDataSource dataSource;

    private FormatIterator<Format> formatIterator = new FormatIterator<>();

    /**
     * @return Connection from the pool. Must be close()d!
     */
    public static synchronized Connection getConnection() throws SQLException {
        if (dataSource == null) {
            final Configuration config = Configuration.forApplication();
            final String jdbcURL       = config.getString(Key.POSTGRESQLSOURCE_JDBC_URL.key(), "");
            final String user          = config.getString(Key.POSTGRESQLSOURCE_USER.key(), "");
            final String password      = config.getString(Key.POSTGRESQLSOURCE_PASSWORD.key(), "");
            // Useful for leak detection
            //System.setProperty("com.zaxxer.hikari.housekeeping.periodMs", "1000");
            dataSource = new HikariDataSource();
            dataSource.setJdbcUrl(jdbcURL);
            dataSource.setUsername(user);
            dataSource.setPassword(password);
            dataSource.setPoolName(PostgreSQLSource.class.getSimpleName() + "Pool");
            dataSource.setMaximumPoolSize(getMaxPoolSize());
            dataSource.setConnectionTimeout(getConnectionTimeout());
            //dataSource.setLeakDetectionThreshold(4000);

            // Create a connection in order to log some things and check
            // whether the database is sane.
            try (Connection connection = dataSource.getConnection()) {
                LOGGER.debug("Using {} {}", connection.getMetaData().getDriverName(),
                        connection.getMetaData().getDriverVersion());
                LOGGER.debug("Connection string: {}",
                        config.getString(Key.POSTGRESQLSOURCE_JDBC_URL.key()));
                LOGGER.debug("Max connection pool size: {}", getMaxPoolSize());
            }
        }
        return dataSource.getConnection();
    }

    /**
     * @return Connection timeout in milliseconds.
     */
    private static int getConnectionTimeout() {
        return 1000 * Configuration.forApplication().getInt(
                Key.POSTGRESQLSOURCE_CONNECTION_TIMEOUT.key(),
                DEFAULT_CONNECTION_TIMEOUT);
    }

    /**
     * @return Maximum connection pool size.
     */
    private static int getMaxPoolSize() {
        return Configuration.forApplication().getInt(
                Key.POSTGRESQLSOURCE_MAX_POOL_SIZE.key(),
                DEFAULT_MAX_POOL_SIZE);
    }

    /**
     * @return Result of the {@link #DATABASE_IDENTIFIER_DELEGATE_METHOD}
     *         method.
     */
    String getDatabaseIdentifier() throws DelegateException {
        return (String) getDelegate()
                .invoke(DATABASE_IDENTIFIER_DELEGATE_METHOD);
    }

    Instant getLastModified() throws IOException {
        try {
            String retval = (String) getDelegate()
                    .invoke(LAST_MODIFIED_DELEGATE_METHOD);
            if (retval != null) {
                // the delegate method result may be an ISO 8601 string, or an
                // SQL statement to look it up.
                if (retval.toUpperCase().startsWith("SELECT")) {
                    // It's called readability, IntelliJ!
                    //noinspection UnnecessaryLocalVariable
                    final String sql = retval;
                    LOGGER.trace(sql);
                    try (Connection connection = getConnection();
                         PreparedStatement statement = connection.prepareStatement(sql)) {
                        statement.setString(1, getDatabaseIdentifier());
                        try (ResultSet resultSet = statement.executeQuery()) {
                            if (resultSet.next()) {
                                Timestamp value = resultSet.getTimestamp(1);
                                if (value != null) {
                                    return value.toInstant();
                                }
                            } else {
                                throw new NoSuchFileException(sql);
                            }
                        }
                    }
                } else {
                    return Instant.parse(retval);
                }
            }
        } catch (SQLException | DelegateException e) {
            throw new IOException(e);
        }
        return null;
    }

    /**
     * @return Result of the {@link #LOOKUP_SQL_DELEGATE_METHOD} method.
     */
    String getLookupSQL() throws IOException, DelegateException {
        final String sql = (String) getDelegate()
                .invoke(LOOKUP_SQL_DELEGATE_METHOD);
        if (!sql.contains("?")) {
            throw new IOException(LOOKUP_SQL_DELEGATE_METHOD +
                    " implementation does not support prepared statements");
        }
        return sql;
    }

    /**
     * @return Result of the {@link #MEDIA_TYPE_DELEGATE_METHOD} method.
     */
    String getMediaType() throws DelegateException {
        return (String) getDelegate().invoke(MEDIA_TYPE_DELEGATE_METHOD);
    }

    private void reset() {
        formatIterator = new FormatIterator<>();
    }

    //endregion
    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Arrays.stream(Key.values())
                .map(Key::toString)
                .filter(k -> k.contains(PostgreSQLSource.class.getSimpleName()))
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
    //region Source methods

    @Override
    public StatResult stat() throws IOException {
        Instant lastModified = getLastModified();
        StatResult result    = new StatResult();
        result.setLastModified(lastModified);
        return result;
    }

    @Override
    public FormatIterator<Format> getFormatIterator() {
        return formatIterator;
    }

    @Override
    public ImageInputStream newInputStream() throws IOException {
        try {
            return new LargeObjectStreamFactory(
                    getLookupSQL(), getDatabaseIdentifier()).newSeekableStream();
        } catch (DelegateException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void setIdentifier(Identifier identifier) {
        super.setIdentifier(identifier);
        reset();
    }

    @Override
    public synchronized void shutdown() {
        synchronized (PostgreSQLSource.class) {
            if (dataSource != null) {
                dataSource.close();
                dataSource = null;
            }
        }
    }

}