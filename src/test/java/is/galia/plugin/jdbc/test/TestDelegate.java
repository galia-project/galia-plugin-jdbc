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

import is.galia.delegate.Delegate;
import is.galia.plugin.Plugin;
import is.galia.resource.RequestContext;

import java.util.Set;

/**
 * Mock implementation. There is a file for this class in {@literal
 * src/test/resources/META-INF/services}.
 */
public class TestDelegate implements Delegate, Plugin {

    public static boolean isApplicationStarted, isApplicationStopped;

    public boolean isPluginInitialized;
    private RequestContext requestContext;

    //region Plugin methods

    @Override
    public Set<String> getPluginConfigKeys() {
        return Set.of();
    }

    @Override
    public String getPluginName() {
        return TestDelegate.class.getSimpleName();
    }

    @Override
    public void onApplicationStart() {
        isApplicationStarted = true;
    }

    @Override
    public void onApplicationStop() {
        isApplicationStopped = true;
    }

    @Override
    public void initializePlugin() {
        isPluginInitialized = true;
    }

    //endregion
    //region Delegate methods

    @Override
    public void setRequestContext(RequestContext context) {
        this.requestContext = context;
    }

    @Override
    public RequestContext getRequestContext() {
        return requestContext;
    }

    //endregion
    //region Custom methods

    public String jdbcsource_database_identifier() {
        return getRequestContext().getIdentifier().toString();
    }

    public String jdbcsource_last_modified() {
        return "SELECT last_modified FROM items WHERE filename = ?";
    }

    public String jdbcsource_media_type() {
        return "SELECT media_type FROM items WHERE filename = ?";
    }

    public String jdbcsource_lookup_sql() {
        return "SELECT image FROM items WHERE filename = ?";
    }

    public String postgresqlsource_database_identifier() {
        return getRequestContext().getIdentifier().toString();
    }

    public String postgresqlsource_last_modified() {
        return "SELECT last_modified FROM items WHERE filename = ?";
    }

    public String postgresqlsource_media_type() {
        return "SELECT media_type FROM items WHERE filename = ?";
    }

    public String postgresqlsource_lookup_sql() {
        return "SELECT image_oid FROM items WHERE filename = ?";
    }

}
