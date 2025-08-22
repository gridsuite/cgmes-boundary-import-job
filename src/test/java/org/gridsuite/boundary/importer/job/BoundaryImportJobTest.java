/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.boundary.importer.job;

import com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer;
import com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer.ExceptionThrowingConsumer;
import jakarta.validation.constraints.NotNull;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockftpserver.fake.FakeFtpServer;
import org.mockftpserver.fake.UserAccount;
import org.mockftpserver.fake.filesystem.DirectoryEntry;
import org.mockftpserver.fake.filesystem.FileEntry;
import org.mockftpserver.fake.filesystem.FileSystem;
import org.mockftpserver.fake.filesystem.UnixFakeFileSystem;
import org.mockserver.client.MockServerClient;
import org.mockserver.junit.jupiter.MockServerExtension;
import org.mockserver.model.MediaType;
import org.mockserver.verify.VerificationTimes;

import java.io.BufferedInputStream;
import java.time.LocalDateTime;
import java.util.Map;

import static com.github.stefanbirkner.fakesftpserver.lambda.FakeSftpServer.withSftpServer;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
@ExtendWith(MockServerExtension.class)
class BoundaryImportJobTest {
    @Test
    void testSftpAcquisition() throws Exception {
        withSftp(server -> {
            server.putFile("boundaries/20210325T1030Z__ENTSOE_BD_001.zip", "fake file content 1", UTF_8);
            server.putFile("boundaries/20210328T0030Z__ENTSOE_BD_006.zip", "fake file content 2", UTF_8);
            server.putFile("boundaries/20210328T0030Z__ENTSOE_BD_006.doc", "fake file content 3", UTF_8);
            server.putFile("boundaries/20210328T0030Z__ENTSOE_BD_aaa.zip", "fake file content 4", UTF_8);
            server.putFile("boundaries/20210328T0030Z__ENTSOE_XX_aaa.zip", "fake file content 5", UTF_8);
            server.putFile("boundaries/20210328T0030Z_FOO_ENTSOE_BD_007.zip", "fake file content 6", UTF_8);

            final String acquisitionServerUrl = "sftp://localhost:" + server.getPort();
            try (final BoundaryAcquisitionServer boundaryAcquisitionServer = new BoundaryAcquisitionServer(acquisitionServerUrl, "dummy", "dummy")) {
                boundaryAcquisitionServer.open();
                Map<String, String> retrievedFiles = boundaryAcquisitionServer.listFiles("./boundaries");
                assertEquals(2, retrievedFiles.size());

                TransferableFile file1 = boundaryAcquisitionServer.getFile("20210325T1030Z__ENTSOE_BD_001.zip", acquisitionServerUrl + "/boundaries/20210325T1030Z__ENTSOE_BD_001.zip");
                assertEquals("20210325T1030Z__ENTSOE_BD_001.zip", file1.getName());
                assertEquals("fake file content 1", new String(file1.getData(), UTF_8));

                TransferableFile file2 = boundaryAcquisitionServer.getFile("20210328T0030Z__ENTSOE_BD_006.zip", acquisitionServerUrl + "/boundaries/20210328T0030Z__ENTSOE_BD_006.zip");
                assertEquals("20210328T0030Z__ENTSOE_BD_006.zip", file2.getName());
                assertEquals("fake file content 2", new String(file2.getData(), UTF_8));
            }
        });
    }

    @Test
    void testFtpAcquisition() throws Exception {
        FileSystem fileSystem = new UnixFakeFileSystem();
        fileSystem.add(new DirectoryEntry("/boundaries"));
        fileSystem.add(new FileEntry("/boundaries/20210325T1030Z__ENTSOE_BD_001.zip", "fake file content 1"));
        fileSystem.add(new FileEntry("/boundaries/20210328T0030Z__ENTSOE_BD_006.zip", "fake file content 2"));
        fileSystem.add(new FileEntry("/boundaries/20210328T0030Z__ENTSOE_BD_006.doc", "fake file content 3"));
        fileSystem.add(new FileEntry("/boundaries/20210328T0030Z__ENTSOE_BD_aaa.zip", "fake file content 4"));
        fileSystem.add(new FileEntry("/boundaries/20210328T0030Z__ENTSOE_XX_aaa.zip", "fake file content 5"));
        fileSystem.add(new FileEntry("/boundaries/20210328T0030Z_FOO_ENTSOE_BD_007.zip", "fake file content 6"));

        FakeFtpServer fakeFtpServer = new FakeFtpServer();
        fakeFtpServer.addUserAccount(new UserAccount("dummy_ftp", "dummy_ftp", "/"));
        fakeFtpServer.setFileSystem(fileSystem);
        fakeFtpServer.setServerControlPort(0);

        fakeFtpServer.start();

        String acquisitionServerUrl = "ftp://localhost:" + fakeFtpServer.getServerControlPort();
        try (BoundaryAcquisitionServer boundaryAcquisitionServer = new BoundaryAcquisitionServer(acquisitionServerUrl, "dummy_ftp", "dummy_ftp")) {
            boundaryAcquisitionServer.open();
            Map<String, String> retrievedFiles = boundaryAcquisitionServer.listFiles("./boundaries");
            assertEquals(2, retrievedFiles.size());

            TransferableFile file1 = boundaryAcquisitionServer.getFile("20210325T1030Z__ENTSOE_BD_001.zip", acquisitionServerUrl + "/boundaries/20210325T1030Z__ENTSOE_BD_001.zip");
            assertEquals("20210325T1030Z__ENTSOE_BD_001.zip", file1.getName());
            assertEquals("fake file content 1", new String(file1.getData(), UTF_8));

            TransferableFile file2 = boundaryAcquisitionServer.getFile("20210328T0030Z__ENTSOE_BD_006.zip", acquisitionServerUrl + "/boundaries/20210328T0030Z__ENTSOE_BD_006.zip");
            assertEquals("20210328T0030Z__ENTSOE_BD_006.zip", file2.getName());
            assertEquals("fake file content 2", new String(file2.getData(), UTF_8));
        } finally {
            fakeFtpServer.stop();
        }
    }

    private static void addGetBoundariesInfosExpectation(MockServerClient mockServerClient, int status, String json) {
        mockServerClient.when(request().withMethod("GET").withPath("/v1/boundaries/infos"))
            .respond(response().withStatusCode(status).withContentType(MediaType.JSON_UTF_8).withBody(json));
    }

    private static void addPostBoundaryExpectation(MockServerClient mockServerClient, int status) {
        mockServerClient.when(request().withMethod("POST").withPath("/v1/boundaries"))
            .respond(response().withStatusCode(status));
    }

    @Test
    void testBoundaryImportRequester(final MockServerClient mockServerClient) throws Exception {
        CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester("http://localhost:" + mockServerClient.getPort() + "/");
        addPostBoundaryExpectation(mockServerClient, 200);
        assertTrue(cgmesBoundaryServiceRequester.importBoundary(new TransferableFile("20210325T1030Z__ENTSOE_EQBD_001.xml", "Boundary file content".getBytes(UTF_8))));

        mockServerClient.clear(request());
        addPostBoundaryExpectation(mockServerClient, 500);
        assertFalse(cgmesBoundaryServiceRequester.importBoundary(new TransferableFile("20210328T0030Z__ENTSOE_TPBD_006.xml", "Boundary file content".getBytes(UTF_8))));
    }

    @Test
    void mainTest(final MockServerClient mockServerClient) throws Exception {
        withSftp(server -> {
            putFile(server, "/20210315T0000Z__ENTSOE_BD_002.zip");

            // 1 zip container for boundaries on SFTP server, no boundary already present in cgmes boundary server, 2 boundaries will be imported
            addGetBoundariesInfosExpectation(mockServerClient, 200, "[]");
            addPostBoundaryExpectation(mockServerClient, 200);

            runJob(server, mockServerClient);
            mockServerClient.verify(request().withMethod("POST").withPath("/v1/boundaries"), VerificationTimes.exactly(2));

            // 1 boundary already present in cgmes boundary server, only 1 boundary will be imported
            mockServerClient.clear(request());
            addGetBoundariesInfosExpectation(mockServerClient, 200, "[{\"id\":\"urn:uuid:22222222-bbbb-bbbb-bbbb-bbbbbbbbbbbb\",\"filename\":\"20210315T0000Z__ENTSOE_TPBD_002.xml\",\"scenarioTime\":\"2020-02-02T00:00\"}]");
            addPostBoundaryExpectation(mockServerClient, 200);
            runJob(server, mockServerClient);
            mockServerClient.verify(request().withMethod("POST").withPath("/v1/boundaries"), VerificationTimes.exactly(1));

            // all boundaries already present in cgmes boundary server, no boundary will be imported
            mockServerClient.clear(request());
            addGetBoundariesInfosExpectation(mockServerClient, 200, "[{\"id\":\"urn:uuid:22222222-bbbb-bbbb-bbbb-bbbbbbbbbbbb\",\"filename\":\"20210315T0000Z__ENTSOE_TPBD_002.xml\",\"scenarioTime\":\"2020-02-02T00:00\"},{\"id\":\"urn:uuid:11111111-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"filename\":\"20210315T0000Z__ENTSOE_EQBD_002.xml\",\"scenarioTime\":\"2020-02-02T18:35\"}]");
            addPostBoundaryExpectation(mockServerClient, 200);
            runJob(server, mockServerClient);
            mockServerClient.verify(request().withMethod("POST").withPath("/v1/boundaries"), VerificationTimes.exactly(0));

            // test error case when retrieving boundaries infos
            mockServerClient.clear(request());
            addGetBoundariesInfosExpectation(mockServerClient, 500, "[]");
            runJob(server, mockServerClient);
        });
    }

    private static void withSftp(@NotNull final ExceptionThrowingConsumer testCode) throws Exception {
        withSftpServer(server -> {
            server.addUser("dummy", "dummy")/*.setPort(2222)*/;
            server.createDirectory("boundaries");
            testCode.accept(server);
            server.deleteAllFilesAndDirectories();
        });
    }

    private static void putFile(FakeSftpServer server, String filepath) throws Exception {
        try (final BufferedInputStream bisEQ = new BufferedInputStream(BoundaryImportJobTest.class.getResourceAsStream(filepath))) {
            server.putFile("boundaries" + filepath, bisEQ.readAllBytes());
        }
    }

    private static void runJob(final FakeSftpServer sftpServer, final MockServerClient mockServerClient) {
        BoundaryAcquisitionJob.run(
                "http://localhost:" + mockServerClient.getPort() + "/",
                "sftp://localhost:" + sftpServer.getPort(),
                "dummy",
                "dummy",
                "./boundaries"
        );
    }

    @Test
    void boundaryInfoTest() {
        LocalDateTime date = LocalDateTime.of(2021, 5, 10, 10, 0, 0);
        BoundaryInfo info = new BoundaryInfo("urn:uuid:22222222-bbbb-bbbb-bbbb-bbbbbbbbbbbb", "20210315T0000Z__ENTSOE_TPBD_002.xml", date);
        assertEquals("urn:uuid:22222222-bbbb-bbbb-bbbb-bbbbbbbbbbbb", info.getId());
        assertEquals("20210315T0000Z__ENTSOE_TPBD_002.xml", info.getFilename());
        assertEquals(date, info.getScenarioTime());
    }
}
