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

import is.galia.stream.ClosingFileCacheImageInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.imageio.stream.ImageInputStream;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Instance for BLOB column values.
 */
final class BlobStreamFactory {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(BlobStreamFactory.class);

    private final String sql;
    private final String databaseIdentifier;

    BlobStreamFactory(String sql, String databaseIdentifier) {
        this.sql = sql;
        this.databaseIdentifier = databaseIdentifier;
    }

    ImageInputStream newSeekableStream() throws IOException {
        LOGGER.trace(sql);
        try (Connection connection = JDBCSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, databaseIdentifier);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    return new ClosingFileCacheImageInputStream(
                            result.getBinaryStream(1));
                } else {
                    throw new NoSuchFileException("Resource not found");
                }
            }
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
