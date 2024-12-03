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
import is.galia.plugin.jdbc.cache.JDBCCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Set;

/**
 * Wraps a {@link Blob}'s {@link OutputStream}, for writing an image to a BLOB.
 * The constructor creates a transaction that is committed on close if the
 * stream is {@link CompletableOutputStream#isComplete() completely written}.
 */
public final class ImageBlobOutputStream extends CompletableOutputStream {

    private static final Logger LOGGER = LoggerFactory.
            getLogger(ImageBlobOutputStream.class);

    private final Blob blob;
    private final OutputStream blobOutputStream;
    private final OperationList opList;
    private final Connection connection;
    private final Set<CacheObserver> observers;

    /**
     * Constructor for writing variant images.
     *
     * @param connection
     * @param opList Variant image operation list
     */
    public ImageBlobOutputStream(Connection connection,
                                 OperationList opList,
                                 Set<CacheObserver> observers) throws SQLException {
        this.connection       = connection;
        this.opList           = opList;
        this.blob             = this.connection.createBlob();
        this.blobOutputStream = blob.setBinaryStream(1);
        this.observers        = observers;
        this.connection.setAutoCommit(false);
    }

    @Override
    public void close() throws IOException {
        LOGGER.trace("Closing stream for {}", opList);
        blobOutputStream.close();
        PreparedStatement statement = null;
        try {
            if (isComplete()) {
                final String sql = String.format(
                        "INSERT INTO %s (%s, %s, %s, %s) VALUES (?, ?, ?, ?)",
                        JDBCCache.getVariantImageTableName(),
                        JDBCCache.VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN,
                        JDBCCache.VARIANT_IMAGE_TABLE_IMAGE_COLUMN,
                        JDBCCache.VARIANT_IMAGE_TABLE_LAST_MODIFIED_COLUMN,
                        JDBCCache.VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN);
                LOGGER.trace(sql);
                statement = connection.prepareStatement(sql);
                statement.setString(1, opList.toString());
                statement.setBlob(2, blob);
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
            throw new IOException(e.getMessage(), e);
        } finally {
            if (statement != null) {
                try {
                    statement.close();
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    @Override
    public void flush() throws IOException {
        blobOutputStream.flush();
    }

    @Override
    public void write(int b) throws IOException {
        blobOutputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        blobOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        blobOutputStream.write(b, off, len);
    }

}
