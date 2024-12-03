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
import is.galia.image.Format;
import is.galia.image.Identifier;
import is.galia.image.StatResult;
import is.galia.plugin.jdbc.BaseTest;
import is.galia.plugin.jdbc.config.Key;
import is.galia.plugin.jdbc.test.PostgreSQLSourceUtils;
import is.galia.plugin.jdbc.test.TestUtils;
import is.galia.source.Source;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.imageio.stream.ImageInputStream;
import java.nio.file.NoSuchFileException;
import java.util.NoSuchElementException;
import java.util.Set;

import static is.galia.plugin.jdbc.test.PostgreSQLSourceUtils.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

class PostgreSQLSourceTest extends BaseTest {

    private PostgreSQLSource instance;

    @BeforeAll
    public static void beforeClass() {
        new PostgreSQLSource().onApplicationStart();
    }

    @BeforeEach
    public void setUp() throws Exception {
        super.setUp();

        Configuration config = Configuration.forApplication();
        assumeFalse(config.getString(Key.POSTGRESQLSOURCE_JDBC_URL.key(), "").isBlank());

        PostgreSQLSourceUtils.dropSourceTable();
        PostgreSQLSourceUtils.createSourceTable();
        PostgreSQLSourceUtils.seedSourceTable();

        instance = newInstance();
    }

    @Override
    public void tearDown() throws Exception {
        PostgreSQLSourceUtils.dropSourceTable();
    }

    private PostgreSQLSource newInstance() {
        PostgreSQLSource instance = new PostgreSQLSource();
        instance.initializePlugin();
        Identifier identifier     = new Identifier(IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE);
        Delegate delegate         = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);
        return instance;
    }

    //region Plugin methods

    @Test
    void getPluginConfigKeys() {
        Set<String> keys = instance.getPluginConfigKeys();
        assertFalse(keys.isEmpty());
    }

    @Test
    void getPluginName() {
        assertEquals(PostgreSQLSource.class.getSimpleName(),
                instance.getPluginName());
    }

    //endregion
    //region Source methods

    /* getFormatIterator() */

    @Test
    void getFormatIteratorHasNext() {
        final Identifier identifier =
                new Identifier(IMAGE_WITH_EXTENSION_WITH_MEDIA_TYPE);
        instance.setIdentifier(identifier);

        Delegate delegate = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        PostgreSQLSource.FormatIterator<Format> it = instance.getFormatIterator();
        assertTrue(it.hasNext());
        it.next(); // identifier extension
        assertTrue(it.hasNext());
        it.next(); // media type column
        assertTrue(it.hasNext());
        it.next(); // magic bytes
        assertFalse(it.hasNext());
    }

    @Test
    void getFormatIteratorNext() {
        final Identifier identifier =
                new Identifier(IMAGE_WITH_INCORRECT_EXTENSION_WITHOUT_MEDIA_TYPE);
        instance.setIdentifier(identifier);

        Delegate delegate = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        PostgreSQLSource.FormatIterator<Format> it = instance.getFormatIterator();
        assertEquals(Format.get("jpg"), it.next()); // identifier extension
        assertEquals(Format.UNKNOWN, it.next());    // media type column
        assertEquals(Format.get("png"), it.next()); // magic bytes
        assertThrows(NoSuchElementException.class, it::next);
    }

    @Test
    void getFormatIteratorConsecutiveInvocationsReturnSameInstance() {
        var it = instance.getFormatIterator();
        assertSame(it, instance.getFormatIterator());
    }

    /* getDatabaseIdentifier() */

    @Test
    void getDatabaseIdentifier() throws Exception {
        Identifier identifier = new Identifier("cats.jpg");
        Delegate delegate     = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        String result = instance.getDatabaseIdentifier();
        assertEquals("cats.jpg", result);
    }

    /* getLookupSQL() */

    @Test
    void getLookupSQL() throws Exception {
        Identifier identifier = new Identifier("cats.jpg");
        Delegate delegate     = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        String result = instance.getLookupSQL();
        assertEquals("SELECT image_oid FROM " + SOURCE_TABLE_NAME + " WHERE filename = ?", result);
    }

    /* getMediaType() */

    @Test
    void getMediaType() throws Exception {
        instance.setIdentifier(new Identifier("cats.jpg"));
        String result = instance.getMediaType();
        assertEquals("SELECT media_type FROM " + SOURCE_TABLE_NAME + " WHERE filename = ?", result);
    }

    /* newInputStream() */

    @Test
    void newInputStreamWithPresentImage() throws Exception {
        try (ImageInputStream is = instance.newInputStream()) {
            assertNotNull(is);
        }
    }

    /* stat() */

    @Test
    void statWithPresentReadableImage() throws Exception {
        instance.stat();
    }

    @Test
    void statWithMissingImage() {
        Identifier identifier = new Identifier("bogus");
        Delegate delegate     = TestUtils.newDelegate();
        delegate.getRequestContext().setIdentifier(identifier);
        instance.setDelegate(delegate);
        instance.setIdentifier(identifier);

        assertThrows(NoSuchFileException.class, instance::stat);
    }

    @Test
    void statReturnsCorrectInstance() throws Exception {
        StatResult result = instance.stat();
        assertNotNull(result.getLastModified());
    }

    /**
     * Tests that {@link Source#stat()} can be invoked multiple times without
     * throwing an exception.
     */
    @Test
    void statInvokedMultipleTimes() throws Exception {
        instance.stat();
        instance.stat();
        instance.stat();
    }

}
