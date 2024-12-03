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

import is.galia.delegate.Delegate;
import is.galia.image.Identifier;
import is.galia.plugin.jdbc.BaseTest;
import is.galia.plugin.jdbc.test.JDBCSourceUtils;
import is.galia.plugin.jdbc.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.imageio.stream.ImageInputStream;
import java.nio.file.Files;

import static is.galia.plugin.jdbc.test.JDBCSourceUtils.IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE;
import static org.junit.jupiter.api.Assertions.*;

class BlobStreamFactoryTest extends BaseTest {

    private BlobStreamFactory instance;

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        JDBCSourceUtils.dropSourceTable();
        JDBCSourceUtils.createSourceTable();
        JDBCSourceUtils.seedSourceTable();

        JDBCSource source     = new JDBCSource();
        Identifier identifier = new Identifier(IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE);
        Delegate delegate     = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        source.setDelegate(delegate);
        source.setIdentifier(identifier);

        String sql          = source.getLookupSQL();
        String dbIdentifier = source.getDatabaseIdentifier();
        instance = new BlobStreamFactory(sql, dbIdentifier);
    }

    @Override
    public void tearDown() throws Exception {
        JDBCSourceUtils.dropSourceTable();
    }

    @Test
    void newSeekableStream() throws Exception {
        final long actualSize = Files.size(TestUtils.getFixture("ghost.png"));
        try (ImageInputStream is = instance.newSeekableStream()) {
            int length = 0;
            while (is.read() != -1) {
                length += 1;
            }
            assertEquals(actualSize, length);
        }
    }

}

