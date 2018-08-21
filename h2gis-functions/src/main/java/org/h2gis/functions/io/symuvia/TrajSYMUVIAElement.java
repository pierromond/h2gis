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

/**
 * A class to manage the traj element properties.
 *
 * @author Pierre Aumond
 */
public class TrajSYMUVIAElement {

    private double abs;
    private double acc;
    private long id;
    private double dst;
    private double ord;
    private String type;
    private double vit;


    /**
     * Constructor
     * @param abs Latitude value
     * @param acc Longitude value
     * @param id Longitude value
     * @param dst Longitude value
     * @param ord Longitude value
     * @param type Longitude value
     * @param vit Longitude value
     */
    public TrajSYMUVIAElement(double abs, double acc, double dst,long id, double ord, String type,double vit) {
        this.abs = abs;
        this.acc = acc;
        this.id = id;
        this.dst = dst;
        this.ord = ord;
        this.type = type;
        this.vit = vit;
    }
    /**
     * The id of the element
     *
     * @return
     */
    public long getID() {
        return id;
    }

    /**
     * Set an id to the element
     *
     * @param id
     */
    public void setId(String id) {
        this.id = Long.valueOf(id);
    }

    public double getABS() {
        return abs;
    }

    public double getACC() {
        return acc;
    }

    public double getDST() {
        return dst;
    }

    public double getORD() {
        return ord;
    }

    public double getVIT() {
        return vit;
    }

    /**
     * Type
     *
     * @return
     */
    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }




}
