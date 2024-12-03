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

import is.galia.plugin.jdbc.cache.PostgreSQLCache;
import is.galia.plugin.jdbc.utils.ConnectionUtils;
import is.galia.util.IOUtils;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;

public final class LargeObjectInputStream extends InputStream {

    private final Connection connection;
    private final LargeObject largeObject;
    private final InputStream wrappedStream;

    /**
     * @param oid Large Object OID.
     */
    public LargeObjectInputStream(long oid) throws SQLException {
        this.connection = PostgreSQLCache.getConnection();
        this.connection.setAutoCommit(false);

        LargeObjectManager manager =
                connection.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
        largeObject   = manager.open(oid, LargeObjectManager.READ);
        wrappedStream = largeObject.getInputStream();
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            IOUtils.closeQuietly(wrappedStream);
            IOUtils.closeQuietly(largeObject);
            ConnectionUtils.rollbackQuietly(connection);
            IOUtils.closeQuietly(connection);
        }
    }

    @Override
    public int read() throws IOException {
        return wrappedStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return wrappedStream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return wrappedStream.read(b, off, len);
    }

}
