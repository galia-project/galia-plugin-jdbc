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

package is.galia.plugin.jdbc;

import is.galia.util.ArchiveUtils;
import is.galia.util.MavenUtils;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.zip.ZipEntry;

import static org.junit.jupiter.api.Assertions.*;

class AssemblyTest extends BaseTest {

    @Test
    void testAssembledZipFile() throws Exception {
        final String expectedPackageFilename =
                MavenUtils.readArtifactIDFromPOM() + "-" +
                        MavenUtils.readVersionFromPOM() + ".zip";
        final Path zipFile                 = MavenUtils.assemblePackage(expectedPackageFilename);
        final String zipBasename           = zipFile.getFileName().toString();
        final String zipBasenameWithoutExt = zipBasename.replace(".zip", "");
        final List<ZipEntry> entries       = ArchiveUtils.entries(zipFile);
        final List<String> files           = entries.stream()
                .map(e -> e.getName().replace(zipBasenameWithoutExt + "/", ""))
                .toList();
        assertTrue(files.contains("config.yml.sample"));
        assertTrue(files.contains("delegates.rb.sample"));
        assertTrue(files.contains("lib/" + zipBasenameWithoutExt + ".jar"));
        assertTrue(files.contains("CHANGES.md"));
        assertTrue(files.contains("LICENSE.txt"));
        assertTrue(files.contains("LICENSE-3RD-PARTY.txt"));
        assertTrue(files.contains("README.md"));
        assertTrue(files.contains("UPGRADING.md"));
    }

}
