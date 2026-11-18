/*
 * Copyright 2026 Ivan Abramov
 *
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
package dev.kfkit.kfkrbac;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.utils.MockTime;

/**
 * A {@link RestAccess} over ACL records in memory, with a clock the test
 * moves: for the tests of the extensions, which live in packages of their
 * own. Every call goes to the class that ships.
 */
public final class TestRestAccess implements AutoCloseable {

    /** A self-signed certificate whose subject is {@code CN=alice,O=example}. */
    private static final String ALICE_PEM = """
            -----BEGIN CERTIFICATE-----
            MIIBnDCCAUGgAwIBAgIUPgm4fWtr9tNlBk0u0m7XF2kV0YQwCgYIKoZIzj0EAwIw
            IjEQMA4GA1UECgwHZXhhbXBsZTEOMAwGA1UEAwwFYWxpY2UwIBcNMjYxMTE4MTEx
            MzA0WhgPMjEyNjEwMjUxMTEzMDRaMCIxEDAOBgNVBAoMB2V4YW1wbGUxDjAMBgNV
            BAMMBWFsaWNlMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEVB7fJ3XSpBwzfRH7
            ZoFOWslmCsemANwqLnzBt8nWiQIEudtgwl32enipAZ+eJSlyfLZQ23J+FYxImfLU
            qrRF0qNTMFEwHQYDVR0OBBYEFJL+xxTo9vkF4UYQuP7zZU+YNz5MMB8GA1UdIwQY
            MBaAFJL+xxTo9vkF4UYQuP7zZU+YNz5MMA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZI
            zj0EAwIDSQAwRgIhAJf96UOyQE2UcsGcBajNObMiJWEj7x9J3OIKqr4GIDtHAiEA
            hv4ObjyOVWw7UPEYCvnJYqwm3s16iE0G641OXy9NEhA=
            -----END CERTIFICATE-----
            """;

    private final FakeAclRecords records;
    private final MockTime time = new MockTime();
    private final RestAccess access;

    private TestRestAccess(String superUsers, AclBinding... records) {
        this.records = new FakeAclRecords(records);
        this.access = new RestAccess(this.records, RestAccess.superUsers(superUsers), time,
                new Metrics(time));
    }

    /** Not read yet: {@link #refresh()} reads the records. */
    public static TestRestAccess of(String superUsers, AclBinding... records) {
        return new TestRestAccess(superUsers, records);
    }

    public RestAccess access() {
        return access;
    }

    /** Reads the records now; whether that succeeded. */
    public boolean refresh() {
        return access.refresh();
    }

    /** Creates a record on the "cluster"; in force after the next read. */
    public void add(AclBinding record) {
        records.records.add(record);
    }

    /** Deletes a record from the "cluster"; out of force after the next read. */
    public void remove(AclBinding record) {
        records.records.remove(record);
    }

    /** Every read fails from now on, as it does when the cluster cannot be reached. */
    public void failReads() {
        records.refusal = new AclRecords.Failure("TimeoutException: the cluster did not answer", null);
    }

    public void sleep(long ms) {
        time.sleep(ms);
    }

    /** The binding of a role on the connectors with that prefix. */
    public static AclBinding onConnectors(String role, String member, String prefix) {
        return BindingRecords.record(role, member, RoleBinding.Target.CONNECTORS, prefix, PatternType.PREFIXED);
    }

    /** The binding of a cluster role. */
    public static AclBinding onTheCluster(String role, String member) {
        return BindingRecords.record(role, member, RoleBinding.Target.CLUSTER, null, PatternType.LITERAL);
    }

    /** The record that puts the member in the group: {@code Member:User:alice} on {@code Group:payments}. */
    public static AclBinding membership(String member, String group) {
        return new AclBinding(new ResourcePattern(ResourceType.TOPIC, group, PatternType.LITERAL),
                new AccessControlEntry(BindingCodec.MEMBER_TYPE + ":" + member, "*", AclOperation.ALL,
                        AclPermissionType.ALLOW));
    }

    /** The certificate of {@code CN=alice,O=example}. */
    public static X509Certificate alice() {
        try {
            return (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(new ByteArrayInputStream(ALICE_PEM.getBytes(StandardCharsets.US_ASCII)));
        } catch (CertificateException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void close() {
        access.close();
    }
}
