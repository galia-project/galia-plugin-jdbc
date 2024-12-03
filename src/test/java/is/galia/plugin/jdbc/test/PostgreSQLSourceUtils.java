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

package is.galia.plugin.jdbc.test;

import is.galia.plugin.jdbc.source.PostgreSQLSource;
import is.galia.plugin.jdbc.utils.ConnectionUtils;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

public final class PostgreSQLSourceUtils {

    public static final String IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE              = "png-1.png";
    public static final String IMAGE_WITHOUT_EXTENSION_WITH_MEDIA_TYPE           = "png-2";
    public static final String IMAGE_WITH_EXTENSION_WITHOUT_MEDIA_TYPE           = "png-3.png";
    public static final String IMAGE_WITH_INCORRECT_EXTENSION_WITHOUT_MEDIA_TYPE = "png.jpg";
    public static final String IMAGE_WITHOUT_EXTENSION_OR_MEDIA_TYPE             = "png-4";
    public static final String SOURCE_TABLE_NAME                                 = "items";

    public static void createSourceTable() throws Exception {
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                        "filename VARCHAR(255)," +
                        "media_type VARCHAR(255)," +
                        "last_modified TIMESTAMP," +
                        "image_oid OID);",
                SOURCE_TABLE_NAME);
        try (Connection connection = PostgreSQLSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    public static void seedSourceTable() throws Exception {
        Connection connection = null;
        try {
            connection = PostgreSQLSource.getConnection();
            connection.setAutoCommit(false);
            for (String filename : new String[]{
                    IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE,
                    IMAGE_WITHOUT_EXTENSION_WITH_MEDIA_TYPE,
                    IMAGE_WITH_EXTENSION_WITHOUT_MEDIA_TYPE,
                    IMAGE_WITH_INCORRECT_EXTENSION_WITHOUT_MEDIA_TYPE,
                    IMAGE_WITHOUT_EXTENSION_OR_MEDIA_TYPE}) {
                String sql = "INSERT INTO items (filename, last_modified, media_type, image_oid) " +
                        "VALUES (?, ?, ?, ?);";

                LargeObjectManager manager =
                        connection.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
                long oid = manager.createLO();

                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setString(1, filename);
                    statement.setTimestamp(2, Timestamp.from(Instant.now()));
                    if (IMAGE_WITHOUT_EXTENSION_OR_MEDIA_TYPE.equals(filename) ||
                            IMAGE_WITH_EXTENSION_WITHOUT_MEDIA_TYPE.equals(filename) ||
                            IMAGE_WITH_INCORRECT_EXTENSION_WITHOUT_MEDIA_TYPE.equals(filename)) {
                        statement.setNull(3, Types.VARCHAR);
                    } else {
                        statement.setString(3, "image/jpeg");
                    }
                    statement.setLong(4, oid);
                    statement.executeUpdate();
                }
                try (LargeObject object = manager.open(oid, LargeObjectManager.WRITE);
                     InputStream is = Files.newInputStream(TestUtils.getFixture("ghost.png"));
                     OutputStream os = object.getOutputStream()) {
                    is.transferTo(os);
                }
            }
            connection.commit();
        } catch (Exception e) {
            ConnectionUtils.rollbackQuietly(connection);
            throw e;
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static void dropSourceTable() throws Exception {
        try (Connection conn = PostgreSQLSource.getConnection()) {
            // DELETE * from it first in order to trigger the LOs to be deleted.
            // (DROP TABLE won't fire the trigger.)
            String sql = "DELETE FROM " + SOURCE_TABLE_NAME + ";";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.execute();
            } catch (SQLException e) {
                // It's fine if the table doesn't exist.
                if (!e.getMessage().contains("does not exist")) {
                    throw e;
                }
            }
            sql = "DROP TABLE IF EXISTS " + SOURCE_TABLE_NAME + ";";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.execute();
            } catch (SQLException e) {
                if (!e.getMessage().contains("not found")) {
                    throw e;
                }
            }
        }
    }

    private PostgreSQLSourceUtils() {}

}
