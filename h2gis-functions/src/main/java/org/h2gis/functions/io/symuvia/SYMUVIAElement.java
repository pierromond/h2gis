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

import java.sql.Timestamp;
import java.util.HashMap;

/**
 * A class to manage all common element properties.
 *
 * @author Pierre Aumond
 */
public class SYMUVIAElement {

    private final HashMap<String, String> tags;
    private long  uid;
    private String user;
    private int version, changeset;
    private boolean visible;
    private String name = "";

    public SYMUVIAElement() {
        tags = new HashMap<String, String>();
    }


    /**
     * The user
     *
     * @return
     */
    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setUid(String uid) {
        if (uid != null) {
            this.uid = Long.valueOf(uid);
        }
    }

    /**
     * @return The way name (extracted from tag)
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Way name
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     *
     * @return
     */
    public boolean getVisible() {
        return visible;
    }

    public void setVisible(String visible) {
        if(visible!=null){
            this.visible = Boolean.valueOf(visible);
        }
    }

    /**
     *
     * @return
     */
    public int getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version != null ? Integer.valueOf(version) : 0;
    }

    /**
     *
     * @return
     */

    public void setChangeset(String changeset) {
        if(changeset!=null){
            this.changeset = Integer.valueOf(changeset);
        }
    }

    public HashMap<String, String> getTags() {
        return tags;
    }

}
