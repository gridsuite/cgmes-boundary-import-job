/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.boundary.importer.job;

import com.powsybl.cgmes.model.FullModel;
import com.powsybl.commons.PowsyblException;
import com.powsybl.commons.config.ModuleConfig;
import com.powsybl.commons.config.PlatformConfig;
import com.powsybl.ws.commons.SecuredZipInputStream;
import org.apache.commons.io.FilenameUtils;
import org.gridsuite.boundary.importer.job.utils.CgmesBoundaryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public final class BoundaryAcquisitionJob {

    private static final Logger LOGGER = LoggerFactory.getLogger(BoundaryAcquisitionJob.class);

    private BoundaryAcquisitionJob() {
    }

    private static void importBoundary(byte[] boundaryContent,
                                       List<BoundaryInfo> allBoundaryInfos,
                                       String fileName,
                                       CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester,
                                       List<String> filesImported,
                                       List<String> filesAlreadyImported,
                                       List<String> filesImportFailed) throws IOException, InterruptedException {
        try (Reader reader = new InputStreamReader(new ByteArrayInputStream(boundaryContent))) {
            // parse full model to get the id
            FullModel fullModel = FullModel.parse(reader);
            String id = fullModel.getId();

            if (allBoundaryInfos.stream().noneMatch(b -> b.getId().equals(id))) {
                // import the boundary to the cgmes boundary server
                LOGGER.info("Importing boundary file '{}'...", fileName);

                boolean importOk = cgmesBoundaryServiceRequester.importBoundary(new TransferableFile(fileName, boundaryContent));
                if (importOk) {
                    filesImported.add(fileName);
                } else {
                    filesImportFailed.add(fileName);
                }
            } else {
                filesAlreadyImported.add(fileName);
            }
        }
    }

    private static void handleZipBoundaryContainer(TransferableFile acquiredFile,
                                                   List<BoundaryInfo> allBoundaryInfos,
                                                   CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester,
                                                   List<String> filesImported,
                                                   List<String> filesAlreadyImported,
                                                   List<String> filesImportFailed) throws IOException, InterruptedException {
        String fileName;
        try (SecuredZipInputStream zis = new SecuredZipInputStream(new ByteArrayInputStream(acquiredFile.getData()), CgmesBoundaryUtils.MAX_ZIP_ENTRIES_COUNT, CgmesBoundaryUtils.MAX_ZIP_SIZE)) {
            ZipEntry entry = zis.getNextEntry();
            while (entry != null) {
                if (new File(entry.getName()).getCanonicalPath().startsWith("..")) {
                    throw new IllegalStateException("Entry is trying to leave the target dir: " + entry.getName());
                }

                // Remove repertory name before file name
                fileName = FilenameUtils.getName(entry.getName());

                // Check if it is a boundary file
                if (!fileName.isEmpty() &&
                    (fileName.matches(CgmesBoundaryUtils.EQBD_FILE_REGEX) ||
                     fileName.matches(CgmesBoundaryUtils.TPBD_FILE_REGEX))) {

                    byte[] boundaryContent = zis.readAllBytes();

                    importBoundary(boundaryContent, allBoundaryInfos, fileName, cgmesBoundaryServiceRequester, filesImported, filesAlreadyImported, filesImportFailed);
                }

                entry = zis.getNextEntry();
            }
        }
    }

    public static void main(String... args) {
        final PlatformConfig platformConfig = PlatformConfig.defaultConfig();
        final ModuleConfig moduleConfigAcquisitionServer = platformConfig.getOptionalModuleConfig("acquisition-server").orElseThrow(() -> new PowsyblException("Module acquisition-server not found !!"));
        final ModuleConfig moduleConfigCgmesBoundaryServer = platformConfig.getOptionalModuleConfig("cgmes-boundary-server").orElseThrow(() -> new PowsyblException("Module cgmes-boundary-server not found !!"));
        run(
            moduleConfigCgmesBoundaryServer.getStringProperty("url"),
            moduleConfigAcquisitionServer.getStringProperty("username"),
            moduleConfigAcquisitionServer.getStringProperty("password"),
            moduleConfigAcquisitionServer.getStringProperty("url"),
            moduleConfigAcquisitionServer.getStringProperty("cgmes-boundary-directory")
        );
    }

    public static void run(String cgmesBoundaryServerUrl, String acquisitionServerUrl, String acquisitionServerUsername, String acquisitionServerPassword,
                           String boundaryDirectory) {
        final CgmesBoundaryServiceRequester cgmesBoundaryServiceRequester = new CgmesBoundaryServiceRequester(cgmesBoundaryServerUrl);

        try (BoundaryAcquisitionServer boundaryAcquisitionServer = new BoundaryAcquisitionServer(acquisitionServerUrl, acquisitionServerUsername, acquisitionServerPassword)) {
            boundaryAcquisitionServer.open();

            Map<String, String> filesToAcquire = boundaryAcquisitionServer.listFiles(boundaryDirectory);
            LOGGER.info("{} files found on server", filesToAcquire.size());

            List<String> filesImported = new ArrayList<>();
            List<String> filesAlreadyImported = new ArrayList<>();
            List<String> filesImportFailed = new ArrayList<>();

            if (!filesToAcquire.isEmpty()) {
                // Get all available boundary infos from cgmes boundary server
                List<BoundaryInfo> allBoundaryInfos = cgmesBoundaryServiceRequester.getBoundariesInfos();

                for (Map.Entry<String, String> fileInfo : filesToAcquire.entrySet()) {
                    // get boundary container zip file
                    TransferableFile acquiredFile = boundaryAcquisitionServer.getFile(fileInfo.getKey(), fileInfo.getValue());

                    handleZipBoundaryContainer(acquiredFile, allBoundaryInfos, cgmesBoundaryServiceRequester, filesImported, filesAlreadyImported, filesImportFailed);
                }
            }

            LOGGER.info("===== JOB EXECUTION SUMMARY =====");
            LOGGER.info("{} files already imported", filesAlreadyImported.size());
            LOGGER.info("{} files successfully imported", filesImported.size());
            filesImported.forEach(f -> LOGGER.info("File '{}' successfully imported", f));
            LOGGER.info("{} files import failed", filesImportFailed.size());
            filesImportFailed.forEach(f -> LOGGER.info("File '{}' import failed !!", f));
            LOGGER.info("=================================");
        } catch (InterruptedException exc) {
            LOGGER.error("Job interruption  error: {}", exc.getMessage());
            Thread.currentThread().interrupt();
        } catch (Exception exc) {
            LOGGER.error("Job execution error: {}", exc.getMessage());
        }
    }
}
