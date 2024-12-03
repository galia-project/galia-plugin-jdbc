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

package is.galia.plugin.jdbc.config;

public enum Key {

    JDBCCACHE_CONNECTION_TIMEOUT ("cache.JDBCCache.connection_timeout"),
    JDBCCACHE_INFO_TABLE         ("cache.JDBCCache.info_table"),
    JDBCCACHE_JDBC_URL           ("cache.JDBCCache.url"),
    JDBCCACHE_MAX_POOL_SIZE      ("cache.JDBCCache.max_pool_size"),
    JDBCCACHE_PASSWORD           ("cache.JDBCCache.password"),
    JDBCCACHE_USER               ("cache.JDBCCache.user"),
    JDBCCACHE_VARIANT_IMAGE_TABLE("cache.JDBCCache.variant_image_table"),

    JDBCSOURCE_CONNECTION_TIMEOUT("source.JDBCSource.connection_timeout"),
    JDBCSOURCE_JDBC_URL          ("source.JDBCSource.url"),
    JDBCSOURCE_MAX_POOL_SIZE     ("source.JDBCSource.max_pool_size"),
    JDBCSOURCE_PASSWORD          ("source.JDBCSource.password"),
    JDBCSOURCE_USER              ("source.JDBCSource.user"),

    POSTGRESQLCACHE_CONNECTION_TIMEOUT ("cache.PostgreSQLCache.connection_timeout"),
    POSTGRESQLCACHE_INFO_TABLE         ("cache.PostgreSQLCache.info_table"),
    POSTGRESQLCACHE_JDBC_URL           ("cache.PostgreSQLCache.url"),
    POSTGRESQLCACHE_MAX_POOL_SIZE      ("cache.PostgreSQLCache.max_pool_size"),
    POSTGRESQLCACHE_PASSWORD           ("cache.PostgreSQLCache.password"),
    POSTGRESQLCACHE_USER               ("cache.PostgreSQLCache.user"),
    POSTGRESQLCACHE_VARIANT_IMAGE_TABLE("cache.PostgreSQLCache.variant_image_table"),

    POSTGRESQLSOURCE_CONNECTION_TIMEOUT("source.PostgreSQLSource.connection_timeout"),
    POSTGRESQLSOURCE_JDBC_URL          ("source.PostgreSQLSource.url"),
    POSTGRESQLSOURCE_MAX_POOL_SIZE     ("source.PostgreSQLSource.max_pool_size"),
    POSTGRESQLSOURCE_PASSWORD          ("source.PostgreSQLSource.password"),
    POSTGRESQLSOURCE_USER              ("source.PostgreSQLSource.user");

    private final String key;

    Key(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }

    @Override
    public String toString() {
        return key();
    }

}
