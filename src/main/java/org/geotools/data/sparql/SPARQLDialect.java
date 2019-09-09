package org.geotools.data.sparql;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

import org.geotools.geometry.jts.Geometries;
import org.geotools.jdbc.BasicSQLDialect;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.SQLDialect;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKTWriter;
import org.opengis.feature.type.GeometryDescriptor;


public class SPARQLDialect extends BasicSQLDialect {

	protected SPARQLDialect(JDBCDataStore dataStore) {
		super(dataStore);
		// TODO Auto-generated constructor stub
	}
	
	@Override
    public void initializeConnection(Connection cx) throws SQLException {
        // spatialize the database
        GeoDB.InitGeoDB(cx);
    }

	
	public void encodeGeometryValue(Geometry value, int srid, StringBuffer sql) throws IOException {
        if (value == null || value.isEmpty()) {
            sql.append("\"");
            sql.append(new WKTWriter().write(value)+"\"^^geo:wktLiteral");
        } else {
            sql.append("NULL");
        }
    }

	@Override
	public void encodeGeometryEnvelope(String tableName, String geometryColumn, StringBuffer sql) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Envelope decodeGeometryEnvelope(ResultSet rs, int column, Connection cx) throws SQLException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Geometry decodeGeometryValue(GeometryDescriptor descriptor, ResultSet rs, String column,
			GeometryFactory factory, Connection cx, Hints hints) throws IOException, SQLException {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
    public Class<?> getMapping(ResultSet columnMetaData, Connection cx) throws SQLException {

        String typeName = columnMetaData.getString("TYPE_NAME");
        if ("UUID".equalsIgnoreCase(typeName)) {
            return UUID.class;
        } else if ("BLOB".equalsIgnoreCase(typeName) || "VARBINARY".equalsIgnoreCase(typeName)) {
            String schemaName = columnMetaData.getString("TABLE_SCHEM");
            String tableName = columnMetaData.getString("TABLE_NAME");
            String columnName = columnMetaData.getString("COLUMN_NAME");

            // look up in geometry columns table
            StringBuffer sql = new StringBuffer("SELECT type FROM geometry_columns WHERE ");
            if (schemaName != null) {
                sql.append("f_table_schema = '").append(schemaName).append("'").append(" AND ");
            }
            sql.append("f_table_name = '").append(tableName).append("' AND ");
            sql.append("f_geometry_column = '").append(columnName).append("'");

            Statement st = cx.createStatement();
            try {
                LOGGER.fine(sql.toString());
                ResultSet rs = st.executeQuery(sql.toString());
                try {
                    if (rs.next()) {
                        String type = rs.getString(1);
                        Geometries g = Geometries.getForName(type);
                        if (g != null) {
                            return g.getBinding();
                        }
                        LOGGER.warning("Geometry type " + type + " not supported.");
                    }
                } finally {
                    dataStore.closeSafe(rs);
                }
            } finally {
                dataStore.closeSafe(st);
            }

            // not a geometry blob, return byte[].class
            return byte[].class;
        }

        // do a check for a column remark which marks this as a geometry
        // do this mostly for backwards compatability
        String remark = columnMetaData.getString("REMARKS");
        if (remark != null) {
            Geometries g = Geometries.getForName(remark);
            if (g != null) {
                return g.getBinding();
            }
        }

        return null;
    }

	@Override
	public void encodeGeometryValue(Geometry value, int dimension, int srid, StringBuffer sql) throws IOException {
		// TODO Auto-generated method stub
		
	}

	
	


}
