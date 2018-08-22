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

import org.locationtech.jts.geom.Point;
import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.util.StringUtils;
import org.h2gis.functions.factory.H2GISDBFactory;
import org.junit.After;
import org.junit.AfterClass;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author Erwan Bocher
 */
public class SYMUVIAImportTest {

    private static Connection connection;
    private static final String DB_NAME = "SYMUVIAImportTest";
    private Statement st;

    @BeforeClass
    public static void tearUp() throws Exception {
        connection = H2GISDBFactory.createSpatialDataBase(DB_NAME);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        connection.close();
    }
    @Before
    public void setUpStatement() throws Exception {
        st = connection.createStatement();
    }

    @After
    public void tearDownStatement() throws Exception {
        st.close();
    }


    @Test
    public void importSYMUVIAFile() throws SQLException {
        st.execute("DROP TABLE IF EXISTS SYMUVIA_INST, SYMUVIA_TRAJ;");
        st.execute("CALL SYMUVIARead(" + StringUtils.quoteStringSQL(SYMUVIAImportTest.class.getResource("Lafayette_new_v5_153000_153200_traf.xml").getPath()) + ", 'SYMUVIA');");
        ResultSet rs = st.executeQuery("SELECT count(*) FROM SYMUVIA_INST");
        rs.next();
        assertEquals(120, rs.getInt(1));
        rs.close();
        // Check number
        rs = st.executeQuery("SELECT count(*) FROM SYMUVIA_TRAJ");
        rs.next();
        assertEquals(3092, rs.getInt(1));
        rs.close();


    }
    

}
