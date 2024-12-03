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

import is.galia.plugin.jdbc.cache.JDBCCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class JDBCCacheUtils {

    public static void createCacheTables() throws Exception {
        // Variant image table
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                        "%s VARCHAR(4096) NOT NULL, " +
                        "%s BLOB, " +
                        "%s TIMESTAMP, " +
                        "%s TIMESTAMP);",
                JDBCCache.getVariantImageTableName(),
                JDBCCache.VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN,
                JDBCCache.VARIANT_IMAGE_TABLE_IMAGE_COLUMN,
                JDBCCache.VARIANT_IMAGE_TABLE_LAST_MODIFIED_COLUMN,
                JDBCCache.VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN);

        try (Connection connection = JDBCCache.getConnection()) {
            if (isPostgreSQL(connection)) {
                sql = sql.replaceAll("BLOB", "bytea");
            }
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
            // Info table
            sql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s (" +
                            "%s VARCHAR(4096) NOT NULL, " +
                            "%s VARCHAR(8192) NOT NULL, " +
                            "%s TIMESTAMP, " +
                            "%s TIMESTAMP);",
                    JDBCCache.getInfoTableName(),
                    JDBCCache.INFO_TABLE_IDENTIFIER_COLUMN,
                    JDBCCache.INFO_TABLE_INFO_COLUMN,
                    JDBCCache.INFO_TABLE_LAST_MODIFIED_COLUMN,
                    JDBCCache.INFO_TABLE_LAST_ACCESSED_COLUMN);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }

    private static boolean isPostgreSQL(Connection connection) throws SQLException {
        String name = connection.getMetaData().getDatabaseProductName();
        return name.contains("PostgreSQL");
    }

    public static void dropCacheTables() throws Exception {
        try (Connection conn = JDBCCache.getConnection()) {
            dropTable(conn, JDBCCache.getVariantImageTableName());
            dropTable(conn, JDBCCache.getInfoTableName());
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

    private JDBCCacheUtils() {}

}
