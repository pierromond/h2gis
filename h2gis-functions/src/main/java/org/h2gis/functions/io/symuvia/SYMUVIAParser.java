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

import org.h2.api.ErrorCode;
import org.h2gis.api.EmptyProgressVisitor;
import org.h2gis.api.ProgressVisitor;
import org.h2gis.utilities.JDBCUtilities;
import org.h2gis.utilities.TableLocation;
import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.helpers.XMLReaderFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import org.h2gis.utilities.TableUtilities;

/**
 * Parse an SYMUVIA file and store the elements into a database. The database model
 * contains 10 tables.
 *
 *
 * @author Pierre Aumond
 */
public class SYMUVIAParser extends DefaultHandler {

    private static final int BATCH_SIZE = 100;
    private PreparedStatement instPreparedStmt;
    private PreparedStatement trajPreparedStmt;

    private int instPreparedStmtBatchSize = 0;
    private int trajPreparedStmtBatchSize = 0;


    private InstSYMUVIAElement instSYMUVIAElement;
    private TrajSYMUVIAElement trajSYMUVIAElement;

    private ProgressVisitor progress = new EmptyProgressVisitor();
    private FileChannel fc;
    private long fileSize = 0;
    private long readFileSizeEachNode = 1;
    private long nodeCountProgress = 0;
    // For progression information return
    private static final int AVERAGE_NODE_SIZE = 500;
    private double indice_val=0;

    public SYMUVIAParser() {

    }

    /**
     * Read the SYMUVIA file and create its corresponding tables.
     *
     * @param inputFile
     * @param tableName
     * @param connection
     * @param progress
     * @return
     * @throws SQLException
     */
    public boolean read(Connection connection, String tableName, File inputFile, ProgressVisitor progress) throws SQLException {
        this.progress = progress.subProcess(100);
        // Initialisation
        final boolean isH2 = JDBCUtilities.isH2DataBase(connection.getMetaData());
        boolean success = false;
        TableLocation requestedTable = TableLocation.parse(tableName, isH2);
        String symuviaTableName = requestedTable.getTable();
        checkSYMUVIATables(connection, isH2, requestedTable, symuviaTableName);
        createSYMUVIADatabaseModel(connection, isH2, requestedTable, symuviaTableName);

        FileInputStream fs = null;
        try {
            fs = new FileInputStream(inputFile);
            this.fc = fs.getChannel();
            this.fileSize = fc.size();
            // Given the file size and an average node file size.
            // Skip how many nodes in order to update progression at a step of 1%
            readFileSizeEachNode = Math.max(1, (this.fileSize / AVERAGE_NODE_SIZE) / 100);
            nodeCountProgress = 0;
            XMLReader parser = XMLReaderFactory.createXMLReader();
            parser.setErrorHandler(this);
            parser.setContentHandler(this);
            if(inputFile.getName().endsWith(".xml")) {
                parser.parse(new InputSource(fs));
            } else{
                throw new SQLException("Supported formats are .xml");
            }
            success = true;
        } catch (SAXException ex) {
            throw new SQLException(ex);
        } catch (IOException ex) {
            throw new SQLException("Cannot parse the file " + inputFile.getAbsolutePath(), ex);
        } finally {
            try {
                if (fs != null) {
                    fs.close();
                }
            } catch (IOException ex) {
                throw new SQLException("Cannot close the file " + inputFile.getAbsolutePath(), ex);
            }
            // When the reading ends, close() method has to be called
            if (instPreparedStmt != null) {
                instPreparedStmt.close();
            }
            if (trajPreparedStmt != null) {
                trajPreparedStmt.close();
            }

        }

        return success;
    }

    /**
     * Check if one table already exists
     *
     * @param connection
     * @param isH2
     * @param requestedTable
     * @param symuviaTableName
     * @throws SQLException
     */
    private void checkSYMUVIATables(Connection connection, boolean isH2, TableLocation requestedTable, String symuviaTableName) throws SQLException {
        String[] omsTables = new String[]{SYMUVIATablesFactory.INST,SYMUVIATablesFactory.TRAJ};
        for (String omsTableSuffix : omsTables) {
            String symuviaTable = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + omsTableSuffix, isH2);
            if (JDBCUtilities.tableExists(connection, symuviaTable)) {
                throw new SQLException("The table " + symuviaTable + " already exists.");
            }
        }
    }

    /**
     * Create the OMS data model to store the content of the file
     *
     * @param connection
     * @param isH2
     * @param requestedTable
     * @param symuviaTableName
     * @throws SQLException
     */
    private void createSYMUVIADatabaseModel(Connection connection, boolean isH2, TableLocation requestedTable, String symuviaTableName) throws SQLException {
        String instTableName = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + SYMUVIATablesFactory.INST, isH2);
        instPreparedStmt =  SYMUVIATablesFactory.createInstTable(connection, instTableName, isH2);
        String trajTableName = TableUtilities.caseIdentifier(requestedTable, symuviaTableName + SYMUVIATablesFactory.TRAJ, isH2);
        trajPreparedStmt =  SYMUVIATablesFactory.createTrajTable(connection, trajTableName, isH2);
    }

    @Override
    public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
        if(progress.isCanceled()) {
            throw new SAXException("Canceled by user");
        }
        if (localName.compareToIgnoreCase("INST") == 0) {
            instSYMUVIAElement = new InstSYMUVIAElement(Double.valueOf(attributes.getValue("val")));

        } else if (localName.compareToIgnoreCase("TRAJ") == 0) {
            trajSYMUVIAElement = new TrajSYMUVIAElement(Double.valueOf(attributes.getValue("abs")),Double.valueOf(attributes.getValue("acc")),Double.valueOf(attributes.getValue("dst")),Long.valueOf(attributes.getValue("id")),Double.valueOf(attributes.getValue("ord")),String.valueOf(attributes.getValue("type")),Double.valueOf(attributes.getValue("vit")));
        }
    }

    @Override
    public void endDocument() throws SAXException {
        // Execute remaining batch
        try {
            instPreparedStmtBatchSize = insertBatch(instPreparedStmt, instPreparedStmtBatchSize, 1);
            trajPreparedStmtBatchSize = insertBatch(trajPreparedStmt, trajPreparedStmtBatchSize, 1);
        } catch (SQLException ex) {
            throw new SAXException("Could not insert sql batch", ex);
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {

        if (localName.compareToIgnoreCase("INST") == 0) {
            try {

                instPreparedStmt.setObject(1, instSYMUVIAElement.getVAL());
                indice_val=instSYMUVIAElement.getVAL();
                instPreparedStmt.addBatch();
                instPreparedStmtBatchSize++;
            } catch (SQLException ex) {
                throw new SAXException("Cannot insert the node  :  " + instSYMUVIAElement.getVAL(), ex);
            }
        } else if (localName.compareToIgnoreCase("TRAJ") == 0) {
            try {
                trajPreparedStmt.setObject(1, indice_val);
                trajPreparedStmt.setObject(2, trajSYMUVIAElement.getABS());
                trajPreparedStmt.setObject(3, trajSYMUVIAElement.getACC());
                trajPreparedStmt.setObject(4, trajSYMUVIAElement.getDST());
                trajPreparedStmt.setObject(5, trajSYMUVIAElement.getID());
                trajPreparedStmt.setObject(6, trajSYMUVIAElement.getORD());
                trajPreparedStmt.setString(7, trajSYMUVIAElement.getType());
                trajPreparedStmt.setObject(8, trajSYMUVIAElement.getVIT());
                trajPreparedStmt.addBatch();
                trajPreparedStmtBatchSize++;
            } catch (SQLException ex) {
                throw new SAXException("Cannot insert the traj  :  " + trajSYMUVIAElement.getABS(), ex);
            }
        }
        try {
            insertBatch();
        } catch (SQLException ex) {
            throw new SAXException("Could not insert sql batch", ex);
        }
        if(nodeCountProgress++ % readFileSizeEachNode == 0) {
            // Update Progress
            try {
                progress.setStep((int) (((double) fc.position() / fileSize) * 100));
            } catch (IOException ex) {
                // Ignore
            }
        }
    }

    private void insertBatch() throws SQLException {
        instPreparedStmtBatchSize = insertBatch(instPreparedStmt, instPreparedStmtBatchSize);
        trajPreparedStmtBatchSize = insertBatch(trajPreparedStmt, trajPreparedStmtBatchSize);
    }
    private int insertBatch(PreparedStatement st, int batchSize, int maxBatchSize) throws SQLException {
        if(batchSize >= maxBatchSize) {
            st.executeBatch();
            return 0;
        } else {
            return batchSize;
        }
    }

    private int insertBatch(PreparedStatement st, int batchSize) throws SQLException {
        return insertBatch(st, batchSize, BATCH_SIZE);
    }



}
