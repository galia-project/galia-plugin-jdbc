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

import is.galia.config.Configuration;
import is.galia.delegate.Delegate;
import is.galia.image.Identifier;
import is.galia.plugin.jdbc.BaseTest;
import is.galia.plugin.jdbc.config.Key;
import is.galia.plugin.jdbc.test.PostgreSQLSourceUtils;
import is.galia.plugin.jdbc.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.imageio.stream.ImageInputStream;
import java.nio.file.Files;

import static is.galia.plugin.jdbc.test.PostgreSQLSourceUtils.IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class LargeObjectStreamFactoryTest extends BaseTest {

    private LargeObjectStreamFactory instance;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        Configuration config = Configuration.forApplication();
        assumeFalse(config.getString(Key.POSTGRESQLSOURCE_JDBC_URL.key(), "").isBlank());

        PostgreSQLSourceUtils.dropSourceTable();
        PostgreSQLSourceUtils.createSourceTable();
        PostgreSQLSourceUtils.seedSourceTable();

        PostgreSQLSource source = new PostgreSQLSource();
        Identifier identifier   = new Identifier(IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE);
        Delegate delegate       = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        source.setDelegate(delegate);
        source.setIdentifier(identifier);

        String sql          = source.getLookupSQL();
        String dbIdentifier = source.getDatabaseIdentifier();
        instance = new LargeObjectStreamFactory(sql, dbIdentifier);
    }

    @Override
    public void tearDown() throws Exception {
        PostgreSQLSourceUtils.dropSourceTable();
    }

    @Test
    void newSeekableStream() throws Exception {
        final long actualSize = Files.size(TestUtils.getFixture("ghost.png"));
        try (ImageInputStream is = instance.newSeekableStream()) {
            assertEquals(actualSize, is.length());
        }
    }

}
