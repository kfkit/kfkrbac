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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * A certificate authority of the tests' own and certificates it signed, made
 * with the JDK's {@code keytool}: one per name, with the subject
 * {@code CN=<name>,O=example}, good for a server and for a client, and
 * valid for the host names the workers are reached by. Each name's key
 * store and the trust store that holds the authority are PKCS12 files with
 * the password {@link #PASSWORD}.
 */
final class Certificates {

    static final String PASSWORD = "kfkrbac-test";

    private static final long KEYTOOL_TIMEOUT_SECONDS = 60;

    private final Path dir;

    /**
     * Makes the authority and a certificate for each name.
     *
     * @param hosts the host names and addresses each certificate is valid for
     */
    Certificates(List<String> names, List<String> hosts) {
        try {
            dir = Files.createTempDirectory("kfkrbac-tls");
            keytool("-genkeypair", "-alias", "ca", "-dname", "CN=kfkrbac-test-ca,O=example", "-ext", "bc:c",
                    "-keyalg", "RSA", "-keysize", "2048", "-validity", "2", "-keystore", file("ca.p12"));
            keytool("-exportcert", "-rfc", "-alias", "ca", "-keystore", file("ca.p12"), "-file", file("ca.pem"));
            keytool("-importcert", "-noprompt", "-alias", "ca", "-file", file("ca.pem"), "-keystore",
                    file("trust.p12"));
            String san = "SAN=" + String.join(",", hosts.stream()
                    .map(host -> (host.matches("[0-9.]+") ? "ip:" : "dns:") + host).toList());
            for (String name : names) {
                String store = file(name + ".p12");
                keytool("-genkeypair", "-alias", "me", "-dname", subject(name), "-keyalg", "RSA", "-keysize",
                        "2048", "-validity", "2", "-keystore", store);
                keytool("-certreq", "-alias", "me", "-keystore", store, "-file", file(name + ".csr"));
                keytool("-gencert", "-rfc", "-alias", "ca", "-keystore", file("ca.p12"), "-infile",
                        file(name + ".csr"), "-outfile", file(name + ".pem"), "-validity", "2", "-ext", san,
                        "-ext", "EKU=serverAuth,clientAuth");
                keytool("-importcert", "-noprompt", "-alias", "ca", "-file", file("ca.pem"), "-keystore", store);
                keytool("-importcert", "-alias", "me", "-file", file(name + ".pem"), "-keystore", store);
            }
        } catch (IOException e) {
            throw new IllegalStateException("could not make the certificates: " + e, e);
        }
    }

    /** {@code CN=<name>,O=example}, as the certificate's subject reads. */
    static String subject(String name) {
        return "CN=" + name + ",O=example";
    }

    /** The key store with the name's key and its certificate. */
    Path keyStore(String name) {
        return dir.resolve(name + ".p12");
    }

    /** The trust store that holds the authority. */
    Path trustStore() {
        return dir.resolve("trust.p12");
    }

    /**
     * A TLS context that trusts the authority and, unless the name is
     * {@code null}, presents the name's certificate.
     */
    SSLContext context(String name) {
        try {
            TrustManagerFactory trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(load(trustStore()));
            KeyManagerFactory keys = null;
            if (name != null) {
                keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                keys.init(load(keyStore(name)), PASSWORD.toCharArray());
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keys == null ? null : keys.getKeyManagers(), trust.getTrustManagers(), null);
            return context;
        } catch (IOException | GeneralSecurityException e) {
            throw new IllegalStateException("could not make a TLS context for " + name + ": " + e, e);
        }
    }

    private static KeyStore load(Path file) throws IOException, GeneralSecurityException {
        KeyStore store = KeyStore.getInstance("PKCS12");
        try (InputStream in = Files.newInputStream(file)) {
            store.load(in, PASSWORD.toCharArray());
        }
        return store;
    }

    private String file(String name) {
        return dir.resolve(name).toString();
    }

    private static void keytool(String... arguments) throws IOException {
        List<String> command = new ArrayList<>();
        command.add(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        command.addAll(List.of(arguments));
        command.addAll(List.of("-storetype", "PKCS12", "-storepass", PASSWORD));
        if (List.of(arguments).contains("-genkeypair")) {
            command.addAll(List.of("-keypass", PASSWORD));
        }
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        try {
            if (!process.waitFor(KEYTOOL_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IOException("keytool did not finish: " + command);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while running keytool", e);
        }
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        if (process.exitValue() != 0) {
            throw new IOException("keytool " + arguments[0] + " failed (" + process.exitValue() + "): " + output);
        }
    }
}
