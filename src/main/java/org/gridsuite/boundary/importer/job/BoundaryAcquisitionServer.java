/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.boundary.importer.job;

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.auth.StaticUserAuthenticator;
import org.apache.commons.vfs2.impl.DefaultFileSystemConfigBuilder;
import org.apache.commons.vfs2.impl.StandardFileSystemManager;
import org.apache.commons.vfs2.provider.ftp.FtpFileSystemConfigBuilder;
import org.apache.commons.vfs2.provider.sftp.SftpFileSystemConfigBuilder;
import org.gridsuite.boundary.importer.job.utils.CgmesBoundaryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class BoundaryAcquisitionServer implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BoundaryAcquisitionServer.class);

    private final StandardFileSystemManager fsManager = new StandardFileSystemManager();
    private final FileSystemOptions fsOptions = new FileSystemOptions();
    private static final int CONNECTION_TIMEOUT = 30000;

    private String serverUrl;

    public BoundaryAcquisitionServer(String url, String userName, String password) throws FileSystemException {
        serverUrl = url;

        StaticUserAuthenticator auth = new StaticUserAuthenticator(null, userName, password);
        DefaultFileSystemConfigBuilder.getInstance().setUserAuthenticator(fsOptions, auth);

        SftpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(fsOptions, true);
        SftpFileSystemConfigBuilder.getInstance().setPreferredAuthentications(fsOptions, "publickey,password");
        SftpFileSystemConfigBuilder.getInstance().setConnectTimeoutMillis(fsOptions, CONNECTION_TIMEOUT);

        FtpFileSystemConfigBuilder.getInstance().setUserDirIsRoot(fsOptions, true);
        FtpFileSystemConfigBuilder.getInstance().setConnectTimeout(fsOptions, CONNECTION_TIMEOUT);
        FtpFileSystemConfigBuilder.getInstance().setPassiveMode(fsOptions, true);
    }

    public void open() throws FileSystemException {
        fsManager.init();
    }

    public Map<String, String> listFiles(String acquisitionDirPath) throws IOException {
        FileObject serverRoot = fsManager.resolveFile(serverUrl, fsOptions);
        FileObject acquisitionDirectory = serverRoot.resolveFile(acquisitionDirPath);
        List<FileObject> childrenFiles = Arrays.stream(acquisitionDirectory.getChildren()).filter(f -> {
            try {
                // filter on zip files that matches pattern
                return f.isFile() && CgmesBoundaryUtils.isValidBoundaryContainerFileName(f.getName().getBaseName());
            } catch (FileSystemException e) {
                LOGGER.warn(e.getMessage());
                return false;
            }
        }).collect(Collectors.toList());

        Map<String, String> childrenUrls = new HashMap<>();
        for (FileObject child : childrenFiles) {
            try {
                String childName = child.getName().getBaseName();
                String childUrl = child.getURL().toString();
                childrenUrls.put(childName, childUrl);
            } catch (FileSystemException e) {
                LOGGER.warn(e.getMessage());
            }
        }

        return childrenUrls;
    }

    public TransferableFile getFile(String fileName, String fileUrl) throws IOException {
        FileObject file = fsManager.resolveFile(fileUrl, fsOptions);
        return new TransferableFile(fileName, file.getContent().getByteArray());
    }

    public void close() {
        fsManager.close();
    }
}
