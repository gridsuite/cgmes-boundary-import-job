/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.boundary.importer.job;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public class TransferableFile {

    private final byte[] fileData;

    private final String fileName;

    public TransferableFile(String fileName, byte[] fileData) {
        this.fileName = fileName;
        this.fileData = fileData;
    }

    public String getName() {
        return fileName;
    }

    public byte[] getData() {
        return fileData;
    }
}
