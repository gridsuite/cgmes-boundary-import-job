/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.boundary.importer.job;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class CgmesBoundaryServiceRequester {

    private static final Logger LOGGER = LoggerFactory.getLogger(CgmesBoundaryServiceRequester.class);

    private static final String API_VERSION = "v1";

    private final String serviceUrl;

    private final HttpClient httpClient;

    public CgmesBoundaryServiceRequester(String serviceUrl) {
        this.serviceUrl = serviceUrl;
        httpClient = HttpClient.newHttpClient();
    }

    public boolean importBoundary(TransferableFile boundaryFile) throws IOException, InterruptedException {
        Map<Object, Object> data = new LinkedHashMap<>();
        data.put("file", boundaryFile);

        // Random 256 length string is used as multipart boundary
        String boundary = new BigInteger(256, new Random()).toString();

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serviceUrl + API_VERSION + "/boundaries"))
                .timeout(Duration.ofMinutes(2))
                .header("Content-Type", "multipart/form-data;boundary=" + boundary)
                .POST(ofMimeMultipartData(data, boundary))
                .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        return response.statusCode() == 200;
    }

    private HttpRequest.BodyPublisher ofMimeMultipartData(Map<Object, Object> data,
                                                         String boundary) throws IOException {
        // Result request body
        List<byte[]> byteArrays = new ArrayList<>();

        // Separator with boundary
        byte[] separator = ("--" + boundary + "\r\nContent-Disposition: form-data; name=").getBytes(StandardCharsets.UTF_8);

        // Iterating over data parts
        for (Map.Entry<Object, Object> entry : data.entrySet()) {

            // Opening boundary
            byteArrays.add(separator);

            // If value is type of Path (file) append content type with file name and file binaries, otherwise simply append key=value
            if (entry.getValue() instanceof TransferableFile) {
                var file = (TransferableFile) entry.getValue();
                String mimeType = "application/octet-stream";
                byteArrays.add(("\"" + entry.getKey() + "\"; filename=\"" + file.getName()
                        + "\"\r\nContent-Type: " + mimeType + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
                byteArrays.add(file.getData());
                byteArrays.add("\r\n".getBytes(StandardCharsets.UTF_8));
            } else if (entry.getValue() instanceof Path) {
                var path = (Path) entry.getValue();
                String mimeType = Files.probeContentType(path);
                byteArrays.add(("\"" + entry.getKey() + "\"; filename=\"" + path.getFileName()
                        + "\"\r\nContent-Type: " + mimeType + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
                byteArrays.add(Files.readAllBytes(path));
                byteArrays.add("\r\n".getBytes(StandardCharsets.UTF_8));
            } else {
                byteArrays.add(("\"" + entry.getKey() + "\"\r\n\r\n" + entry.getValue() + "\r\n")
                        .getBytes(StandardCharsets.UTF_8));
            }
        }

        // Closing boundary
        byteArrays.add(("--" + boundary + "--").getBytes(StandardCharsets.UTF_8));

        // Serializing as byte array
        return HttpRequest.BodyPublishers.ofByteArrays(byteArrays);
    }

    public List<String> getBoundariesIds() throws IOException, InterruptedException {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(serviceUrl + API_VERSION + "/boundaries/ids"))
                .GET()
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == 200) {
                String json = response.body();

                List<String> result = new ArrayList<>();
                JSONArray array = new JSONArray(json);
                for (int i = 0; i < array.length(); i++) {
                    result.add(array.getString(i));
                }
                return result;
            }
        } catch (IOException e) {
            LOGGER.error("I/O Error while getting all boundary ids");
        } catch (InterruptedException e) {
            LOGGER.error("Interruption while getting all boundary ids");
            Thread.currentThread().interrupt();
        }
        return Collections.emptyList();
    }
}
