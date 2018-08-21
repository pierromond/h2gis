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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2gis.utilities.TableLocation;
import org.h2gis.utilities.TableUtilities;

/**
 * Class to create the tables to import symuvia data
 * 
 * An SYMUVIA file is stored in 1 table.
 * 
 * (1) table_prefix + _all :  table that contains all
 * 
 * @author Pierre Aumond
 */
public class SYMUVIATablesFactory {

    //Suffix table names
    public static final String TRAJ = "_traj";
    public static final String INST = "_inst";


    private SYMUVIATablesFactory() {

    }
    
    /**
     * Create the global table that will be used to import SYMUVIA nodes
     * @param connection
     * @param instTableName
     * @param isH2
     * @return
     * @throws SQLException
     */
    public static PreparedStatement createInstTable(Connection connection, String instTableName, boolean isH2) throws SQLException {
        Statement stmt = connection.createStatement();
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(instTableName);
        sb.append("(val DOUBLE PRECISION);");
        stmt.execute(sb.toString());
        stmt.close();
        return connection.prepareStatement("INSERT INTO " + instTableName + " VALUES (?);");
    }


    /**
     * Create the global table that will be used to import SYMUVIA nodes
     * @param connection
     * @param trajTableName
     * @param isH2
     * @return
     * @throws SQLException
     */
    public static PreparedStatement createTrajTable(Connection connection, String trajTableName, boolean isH2) throws SQLException {
        Statement stmt = connection.createStatement();
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(trajTableName);
        sb.append("(inst DOUBLE PRECISION,"
                + "abs DOUBLE PRECISION,"
                + "acc DOUBLE PRECISION,"
                + "dst DOUBLE PRECISION,"
                + "id INTEGER,"
                + "ord DOUBLE PRECISION,"
                + "type VARCHAR,"
                + "vit DOUBLE PRECISION);");
        stmt.execute(sb.toString());
        stmt.close();
        return connection.prepareStatement("INSERT INTO " + trajTableName + " VALUES (?,?,?,?,?,?,?,?);");
    }



    /**
     * Drop the existing SYMUVIA tables used to store the imported SYMUVIA data
     *
     * @param connection
     * @param isH2
     * @param tablePrefix
     * @throws SQLException
     */
    public static void dropSYMUVIATables(Connection connection, boolean isH2, String tablePrefix) throws SQLException {
        TableLocation requestedTable = TableLocation.parse(tablePrefix, isH2);
        String symuviaTableName = requestedTable.getTable();
        String[] omsTables = new String[]{INST, TRAJ};
        StringBuilder sb =  new StringBuilder("drop table if exists ");
        String omsTableSuffix = omsTables[0];
        String symuviaTable = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + omsTableSuffix, isH2);
        sb.append(symuviaTable);
        for (int i = 1; i < omsTables.length; i++) {
            omsTableSuffix = omsTables[i];
            symuviaTable = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + omsTableSuffix, isH2);
            sb.append(",").append(symuviaTable);
        }        
        Statement stmt = connection.createStatement();
        stmt.execute(sb.toString());
        stmt.close();
    }
}
