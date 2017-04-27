/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.acl;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.security.Identity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;

public class FileBasedIdentityManager
        implements IdentityManager
{
    private static final String ADMIN_GROUP = "ADMIN";
    private final Map<String, Group> groups;
    private final Map<String, SchemaAdmins> schemas;

    @Inject
    public FileBasedIdentityManager(FileBasedIdentityConfig config)
    {
        ObjectMapper mapper = new ObjectMapper();
        try {
            this.groups = mapper.readValue(new File(config.getGroupsFileName()), new TypeReference<Map<String, Group>>() {});
            this.schemas = mapper.readValue(new File(config.getSchemaOwnersFileName()), new TypeReference<Map<String, SchemaAdmins>>() {});
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "failed to load identity config", e);
        }
    }

    @Override
    public boolean isAdmin(ConnectorTransactionHandle transaction, Identity identity)
    {
        Group admin = groups.get(ADMIN_GROUP);
        if (admin == null || !admin.getUsers().contains(identity.getUser())) {
            return false;
        }
        return true;
    }

    @Override
    public boolean isDatabaseOwner(ConnectorTransactionHandle transaction, Identity identity, String schemaName)
    {
        SchemaAdmins admins = schemas.get(schemaName);
        if (admins == null) {
            return false;
        }
        if (admins.getUsers().contains(identity.getUser())) {
            return true;
        }

        return admins.getRoles().stream()
                .map(groups::get)
                .anyMatch(r -> r.hasUser(identity.getUser()));
    }

    private static class Group
    {
        private final Set<String> users;

        @JsonCreator
        Group(@JsonProperty("users") Set<String> users)
        {
            this.users = users;
        }

        public Set<String> getUsers()
        {
            return users;
        }

        public boolean hasUser(String user)
        {
            return users.contains(user);
        }
    }

    private static class SchemaAdmins
    {
        private final Set<String> roles;
        private final Set<String> users;

        @JsonCreator
        SchemaAdmins(@JsonProperty("groups") Set<String> roles, @JsonProperty("users") Set<String> users)
        {
            this.roles = roles;
            this.users = users;
        }

        public Set<String> getRoles()
        {
            return roles;
        }

        public Set<String> getUsers()
        {
            return users;
        }
    }
}
