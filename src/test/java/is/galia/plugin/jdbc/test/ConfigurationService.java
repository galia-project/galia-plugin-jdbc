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

import is.galia.config.Configuration;
import is.galia.config.ConfigurationFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;

public final class ConfigurationService {

    public static Configuration getConfiguration() {
        try {
            return ConfigurationFactory.fromPath(Paths.get("config.yml"));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ConfigurationService() {}

}
