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

import is.galia.plugin.jdbc.source.PostgreSQLSource;
import is.galia.plugin.jdbc.utils.ConnectionUtils;
import is.galia.util.IOUtils;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.stream.ImageInputStream;
import javax.imageio.stream.ImageInputStreamImpl;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public final class LargeObjectImageInputStream extends ImageInputStreamImpl
        implements ImageInputStream {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(LargeObjectImageInputStream.class);

    private final Connection connection;
    private final LargeObject object;

    /**
     * @param oid Large Object OID.
     */
    public LargeObjectImageInputStream(long oid) throws SQLException {
        connection = PostgreSQLSource.getConnection();
        connection.setAutoCommit(false);
        LargeObjectManager manager = connection.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
        object = manager.open(oid, LargeObjectManager.READ);
    }

    @Override
    public void close() throws IOException {
        try {
            super.close();
        } finally {
            IOUtils.closeQuietly(object);
            ConnectionUtils.rollbackQuietly(connection);
            IOUtils.closeQuietly(connection);
        }
    }

    @Override
    public long length() {
        try {
            return object.size64();
        } catch (SQLException e) {
            LOGGER.error("length(): {}", e.getMessage());
            return 0;
        }
    }

    @Override
    public int read() throws IOException {
        try {
            setBitOffset(0);
            byte[] bytes = object.read(1);
            streamPos += 1;
            return bytes[0];
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        try {
            setBitOffset(0);
            int result = object.read(b, off, len);
            if (result == 0) {
                return -1;
            }
            streamPos += result;
            return result;
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void seek(long pos) throws IOException {
        super.seek(pos);
        try {
            object.seek64(pos, LargeObject.SEEK_SET);
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
