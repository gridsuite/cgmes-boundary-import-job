/**
 * Copyright (c) 2021, RTE (http://www.rte-france.com)
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package org.gridsuite.boundary.importer.job.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Franck Lecuyer <franck.lecuyer at rte-france.com>
 */
public final class CgmesBoundaryUtils {
    public static final int MAX_ZIP_ENTRIES_COUNT = 100;
    public static final int MAX_ZIP_SIZE = 1000000000;
    public static final String TPBD_FILE_REGEX = "^(.*?(__ENTSOE_TPBD_).*(.xml))$";
    public static final String EQBD_FILE_REGEX = "^(.*?(__ENTSOE_EQBD_).*(.xml))$";

    private static final String DOT_REGEX = "[.]";
    private static final String UNDERSCORE_REGEX = "_";
    private static final int VERSION_LENGTH = 3;

    private static final Logger LOGGER = LoggerFactory.getLogger(CgmesBoundaryUtils.class);

    private CgmesBoundaryUtils() {
    }

    /* The boundary zip container file name should have the following structure:
    <effectiveDateTime>__ENTSOE_BD_<fileVersion>.zip
    where :
    <effectiveDateTime>: UTC datetime (YYYYMMDDTHHmmZ)
    <fileVersion>: three characters long positive integer number between 000 and 999
     */
    public static boolean isValidBoundaryContainerFileName(String filename) {
        if (filename.split(DOT_REGEX).length == 2) {
            String base = filename.split(DOT_REGEX)[0];
            String ext = filename.split(DOT_REGEX)[1];
            if (ext.equals("zip") && base.split(UNDERSCORE_REGEX).length == 5) {
                String[] parts = base.split(UNDERSCORE_REGEX);
                if (parts[1].isEmpty() &&
                    parts[2].equals("ENTSOE") &&
                    parts[3].equals("BD") &&
                    isValidFileVersion(parts[4])) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean isValidFileVersion(String version) {
        try {
            int v = Integer.parseInt(version);
            if (version.length() != VERSION_LENGTH) {
                LOGGER.warn("File version length is {} and it should be {}", version.length(), VERSION_LENGTH);
                return false;
            }
            return v > 0 && v < 1000;
        } catch (NumberFormatException e) {
            LOGGER.warn("Invalid file version {}", version);
            return false;
        }
    }
}
