/**
 * H2GIS is a library that brings spatial support to the H2 Database Engine
 * <http://www.h2database.com>. H2GIS is developed by CNRS
 * <http://www.cnrs.fr/>.
 *
 * This code is part of the H2GIS project. H2GIS is free software; 
 * you can redistribute it and/or modify it under the terms of the GNU
 * Lesser General Public License as published by the Free Software Foundation;
 * version 3.0 of the License.
 *
 * H2GIS is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License
 * for more details <http://www.gnu.org/licenses/>.
 *
 *
 * For more information, please consult: <http://www.h2gis.org/>
 * or contact directly: info_at_h2gis.org
 */

package org.h2gis.functions.io.symuvia;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.h2gis.api.DriverFunction;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.utilities.JDBCUtilities;

/**
 *
 * @author Pierre Aumond
 */
public class SYMUVIADriverFunction implements DriverFunction {

    public static String DESCRIPTION = "SYMUVIA file (ver. x.x)";

    @Override
    public IMPORT_DRIVER_TYPE getImportDriverType() {
        return IMPORT_DRIVER_TYPE.COPY;
    }

    @Override
    public String[] getExportFormats() {
        return new String[0];
    }

    @Override
    public String getFormatDescription(String format) {
        if (format.equalsIgnoreCase("xml")) {
            return DESCRIPTION;
        }  else {
            return "";
        }
    }

    @Override
    public boolean isSpatialFormat(String extension) {
        return extension.equalsIgnoreCase("xml");
    }

    @Override
    public void exportTable(Connection connection, String tableReference, File fileName, ProgressVisitor progress) throws SQLException, IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void importFile(Connection connection, String tableReference, File fileName, ProgressVisitor progress) throws SQLException, IOException {
        importFile(connection, tableReference, fileName, progress, false);
    }
    
    
     /**
     *
     * @param connection Active connection, do not close this connection.
     * @param tableReference prefix uses to store the SYMUVIA tables
     * @param fileName File path to read
     * @param progress
     * @param deleteTables  true to delete the existing tables
     * @throws SQLException Table write error
     * @throws IOException File read error
     */
    public void importFile(Connection connection, String tableReference, File fileName, ProgressVisitor progress, boolean deleteTables) throws SQLException, IOException {
        if(fileName == null || !(fileName.getName().endsWith(".xml") )) {
            throw new IOException(new IllegalArgumentException("This driver handle only .xml files"));
        }
        if(deleteTables){
            SYMUVIATablesFactory.dropSYMUVIATables(connection, JDBCUtilities.isH2DataBase(connection.getMetaData()), tableReference);
        }
        SYMUVIAParser symuviap = new SYMUVIAParser();
        symuviap.read(connection, tableReference, fileName, progress);
    }

    @Override
    public String[] getImportFormats() {
        return new String[]{"xml"};
    }

}
