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

import is.galia.plugin.jdbc.cache.PostgreSQLCache;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public final class PostgreSQLCacheUtils {

    public static void createCacheTables() throws Exception {
        // Variant image table
        String sql = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                        "%s VARCHAR(4096) NOT NULL, " +
                        "%s OID NOT NULL, " +
                        "%s TIMESTAMP, " +
                        "%s TIMESTAMP);",
                PostgreSQLCache.getVariantImageTableName(),
                PostgreSQLCache.VARIANT_IMAGE_TABLE_OPERATIONS_COLUMN,
                PostgreSQLCache.VARIANT_IMAGE_TABLE_IMAGE_OID_COLUMN,
                PostgreSQLCache.VARIANT_IMAGE_TABLE_LAST_MODIFIED_COLUMN,
                PostgreSQLCache.VARIANT_IMAGE_TABLE_LAST_ACCESSED_COLUMN);
        try (Connection connection = PostgreSQLCache.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
        // Info table
        sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "%s VARCHAR(4096) NOT NULL, " +
                        "%s VARCHAR(8192) NOT NULL, " +
                        "%s TIMESTAMP, " +
                        "%s TIMESTAMP);",
                PostgreSQLCache.getInfoTableName(),
                PostgreSQLCache.INFO_TABLE_IDENTIFIER_COLUMN,
                PostgreSQLCache.INFO_TABLE_INFO_COLUMN,
                PostgreSQLCache.INFO_TABLE_LAST_MODIFIED_COLUMN,
                PostgreSQLCache.INFO_TABLE_LAST_ACCESSED_COLUMN);
        try (Connection connection = PostgreSQLCache.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.execute();
        }
    }

    public static void dropCacheTables() throws Exception {
        try (Connection conn = PostgreSQLCache.getConnection()) {
            dropTable(conn, PostgreSQLCache.getVariantImageTableName());
            dropTable(conn, PostgreSQLCache.getInfoTableName());
        }
    }

    private static void dropTable(Connection conn,
                                  String name) throws SQLException {
        // DELETE * from it first in order to trigger the LOs to be deleted.
        // (DROP TABLE won't fire the trigger.)
        String sql = "DELETE FROM " + name + ";";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            // It's fine if the table doesn't exist.
            if (!e.getMessage().contains("does not exist")) {
                throw e;
            }
        }
        sql = "DROP TABLE IF EXISTS " + name + ";";
        try (PreparedStatement statement = conn.prepareStatement(sql)) {
            statement.execute();
        } catch (SQLException e) {
            if (!e.getMessage().contains("not found")) {
                throw e;
            }
        }
    }

    private PostgreSQLCacheUtils() {}

}
