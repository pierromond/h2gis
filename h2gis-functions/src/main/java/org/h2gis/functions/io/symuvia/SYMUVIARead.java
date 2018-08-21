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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.h2gis.api.AbstractFunction;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ScalarFunction;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.URIUtilities;

/**
 * SQL Function to copy SYMUVIA File data into a set of tables.
 *
 * @author Pierre Aumond
 */
public class SYMUVIARead extends AbstractFunction implements ScalarFunction {

    public SYMUVIARead() {
        addProperty(PROP_REMARKS, "Read a SYMUVIA file and copy the content in the specified tables.\n"
                + " table SYMUVIA must be dropped.");
    }

    @Override
    public String getJavaStaticMethod() {
        return "readSYMUVIA";
    }
    
    /**
     * 
     * @param connection
     * @param fileName
     * @param tableReference
     * @param deleteTables  true to delete the existing tables
     * @throws FileNotFoundException
     * @throws SQLException 
     */
    public static void readSYMUVIA(Connection connection, String fileName, String tableReference, boolean deleteTables) throws FileNotFoundException, SQLException, IOException {
        if(deleteTables){
            SYMUVIATablesFactory.dropSYMUVIATables(connection, JDBCUtilities.isH2DataBase(connection.getMetaData()), tableReference);
        }        
        File file = URIUtilities.fileFromString(fileName);
        if (!file.exists()) {
            throw new FileNotFoundException("The following file does not exists:\n" + fileName);
        }
        SYMUVIADriverFunction symuviadf = new SYMUVIADriverFunction();
        symuviadf.importFile(connection, tableReference, file, new EmptyProgressVisitor(), deleteTables);
    }

    /**
     * 
     * @param connection
     * @param fileName
     * @param tableReference
     * @throws FileNotFoundException
     * @throws SQLException 
     */
    public static void readSYMUVIA(Connection connection, String fileName, String tableReference) throws FileNotFoundException, SQLException, IOException {
        readSYMUVIA(connection, fileName, tableReference, false);
    }

    /**
     * 
     * @param connection
     * @param fileName
     * @throws FileNotFoundException
     * @throws SQLException 
     */
    public static void readSYMUVIA(Connection connection, String fileName) throws FileNotFoundException, SQLException, IOException {
        final String name = URIUtilities.fileFromString(fileName).getName();
        readSYMUVIA(connection, fileName, name.substring(0, name.lastIndexOf(".")).toUpperCase());
    }

}
