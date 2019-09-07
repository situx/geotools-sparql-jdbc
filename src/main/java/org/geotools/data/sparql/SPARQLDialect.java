package org.geotools.data.sparql;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.SQLDialect;
import org.geotools.util.factory.Hints;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;


public class SPARQLDialect extends SQLDialect {

	protected SPARQLDialect(JDBCDataStore dataStore) {
		super(dataStore);
		// TODO Auto-generated constructor stub
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
	
	


}
