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

package is.galia.plugin.jdbc.stream;

import is.galia.cache.CacheObserver;
import is.galia.stream.CompletableOutputStream;
import is.galia.operation.OperationList;
import is.galia.plugin.jdbc.cache.PostgreSQLCache;
import is.galia.plugin.jdbc.utils.ConnectionUtils;
import is.galia.util.IOUtils;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;

/**
 * Writes data to a Large Object using its standard output stream interface.
 * The constructor creates a transaction that is committed on close if the
 * stream is {@link CompletableOutputStream#isComplete() completely written}.
 */
public final class LargeObjectOutputStream extends CompletableOutputStream {

    private static final Logger LOGGER = LoggerFactory.
            getLogger(LargeObjectOutputStream.class);

    private final LargeObject object;
    private final OperationList opList;
    private final Connection connection;
    private final Set<CacheObserver> observers;

    /**
     * @param opList Variant image operation list.
     * @param observers Instances to notify when the writing is complete.
     */
    public LargeObjectOutputStream(OperationList opList,
                                   Set<CacheObserver> observers) throws SQLException {
        this.connection            = PostgreSQLCache.getConnection();
        this.connection.setAutoCommit(false);
        this.opList                = opList;
        LargeObjectManager manager = connection.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
        long oid                   = manager.createLO();
        this.object                = manager.open(oid, LargeObjectManager.WRITE);
        this.observers             = observers;
    }

    @Override
    public void close() throws IOException {
        LOGGER.trace("Closing stream for {}", opList);
        PreparedStatement statement = null;
        try {
            IOUtils.closeQuietly(object);
            if (isComplete()) {
                final String sql = String.format(
                        "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
                        PostgreSQLCache.getVariantImageTableName(),
                        PostgreSQLCache.VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN,
                        PostgreSQLCache.VARIANT_IMAGE_TABLE_IMAGE_OID_COLUMN,
                        PostgreSQLCache.VARIANT_IMAGE_TABLE_LAST_MODIFIED_COLUMN,
                        PostgreSQLCache.VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN);
                LOGGER.trace(sql);
                statement = connection.prepareStatement(sql);
                statement.setString(1, opList.toString());
                statement.setLong(2, object.getLongOID());
                Timestamp now = Timestamp.from(Instant.now());
                statement.setTimestamp(3, now);
                statement.setTimestamp(4, now);
                statement.executeUpdate();
                connection.commit();
                observers.forEach(o -> o.onImageWritten(opList));
            } else {
                connection.rollback();
            }
        } catch (SQLException e) {
            ConnectionUtils.rollbackQuietly(connection);
            throw new IOException(e);
        } finally {
            IOUtils.closeQuietly(statement);
            IOUtils.closeQuietly(connection);
        }
    }

    @Override
    public void write(int b) throws IOException {
        try {
            object.write(new byte[] { (byte) b });
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void write(byte[] b) throws IOException {
        try {
            object.write(b);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        try {
            object.write(b, off, len);
        } catch (SQLException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

}
