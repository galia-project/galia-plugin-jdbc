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

import is.galia.plugin.jdbc.source.JDBCSource;

import java.io.InputStream;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;

public final class JDBCSourceUtils {

    public static final String IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE              = "png-1.png";
    public static final String IMAGE_WITHOUT_EXTENSION_WITH_MEDIA_TYPE           = "png-2";
    public static final String IMAGE_WITH_EXTENSION_WITHOUT_MEDIA_TYPE           = "png-3.png";
    public static final String IMAGE_WITH_INCORRECT_EXTENSION_WITHOUT_MEDIA_TYPE = "png.jpg";
    public static final String IMAGE_WITHOUT_EXTENSION_OR_MEDIA_TYPE             = "png-4";
    public static final String SOURCE_TABLE_NAME                                 = "items";

    public static void createSourceTable() throws Exception {
        try (Connection connection = JDBCSource.getConnection()) {
            String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                            "filename VARCHAR(255)," +
                            "media_type VARCHAR(255)," +
                            "last_modified TIMESTAMP," +
                            "image BLOB);",
                    SOURCE_TABLE_NAME);
            if (isPostgreSQL(connection)) {
                sql = sql.replaceAll("BLOB", "bytea");
            }
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }

    private static boolean isPostgreSQL(Connection connection) throws SQLException {
        String name = connection.getMetaData().getDatabaseProductName();
        return name.contains("PostgreSQL");
    }

    public static void seedSourceTable() throws Exception {
        try (Connection connection = JDBCSource.getConnection()) {
            for (String filename : new String[]{
                    IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE,
                    IMAGE_WITHOUT_EXTENSION_WITH_MEDIA_TYPE,
                    IMAGE_WITH_EXTENSION_WITHOUT_MEDIA_TYPE,
                    IMAGE_WITH_INCORRECT_EXTENSION_WITHOUT_MEDIA_TYPE,
                    IMAGE_WITHOUT_EXTENSION_OR_MEDIA_TYPE}) {
                String sql = "INSERT INTO items (filename, last_modified, media_type, image) " +
                        "VALUES (?, ?, ?, ?);";

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
                    try (InputStream is = Files.newInputStream(TestUtils.getFixture("ghost.png"))) {
                        // N.B.: the MariaDB JDBC driver is known to have
                        // issues with setBlob()/setBinaryStream()
                        byte[] bytes = is.readAllBytes();
                        statement.setBytes(4, bytes);
                    }
                    statement.executeUpdate();
                }
            }
        }
    }

    public static void dropSourceTable() throws Exception {
        try (Connection conn = JDBCSource.getConnection()) {
            dropTable(conn, SOURCE_TABLE_NAME);
        }
    }

    private static void dropTable(Connection conn,
                                  String name) throws SQLException {
        String sql = "DROP TABLE IF EXISTS " + name + ";";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            if (!e.getMessage().contains("not found")) {
                throw e;
            }
        }
    }

    private JDBCSourceUtils() {}

}
