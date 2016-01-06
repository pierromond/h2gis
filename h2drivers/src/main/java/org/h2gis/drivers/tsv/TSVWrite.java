/**
 * H2GIS is a library that brings spatial support to the H2 Database Engine
 * <http://www.h2database.com>.
 *
 * H2GIS is distributed under GPL 3 license. It is produced by CNRS
 * <http://www.cnrs.fr/>.
 *
 * H2GIS is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 *
 * H2GIS is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * H2GIS. If not, see <http://www.gnu.org/licenses/>.
 *
 * For more information, please consult: <http://www.h2gis.org/>
 * or contact directly: info_at_h2gis.org
 */

package org.h2gis.drivers.tsv;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.h2gis.h2spatialapi.AbstractFunction;
import org.h2gis.h2spatialapi.EmptyProgressVisitor;
import org.h2gis.h2spatialapi.ScalarFunction;
import org.h2gis.utilities.URIUtility;

/**
 * Write a Tab-separated values file
 * @author Erwan Bocher
 */
public class TSVWrite extends AbstractFunction implements ScalarFunction {

    public TSVWrite() {
        addProperty(PROP_REMARKS, "Write a Tab-separated values file.");
    }

    @Override
    public String getJavaStaticMethod() {
        return "writeTSV";
    }

    /**
     * Export a table into a Tab-separated values file
     *
     * @param connection
     * @param fileName
     * @param tableReference
     * @throws SQLException
     * @throws IOException
     */
    public static void writeTSV(Connection connection, String fileName, String tableReference) throws SQLException, IOException {
        TSVDriverFunction tSVDriverFunction = new TSVDriverFunction();
        tSVDriverFunction.exportTable(connection, tableReference, URIUtility.fileFromString(fileName), new EmptyProgressVisitor());
    }
}
