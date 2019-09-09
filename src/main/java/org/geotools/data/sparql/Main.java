package org.geotools.data.sparql;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;

import org.apache.jena.jdbc.mem.MemDriver;
import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.jdbc.JDBCDataStore;

public class Main {

	public static void main(String[] args) throws IOException, SQLException {
		MemDriver.register();
		java.sql.Connection conn = DriverManager.getConnection("jdbc:jena:remote:query=http://query.wikidata.org/sparql");
		Statement stmt = conn.createStatement();

		try {
		  // Make a query
		  ResultSet rset = stmt.executeQuery("SELECT DISTINCT ?type WHERE { ?s a ?type } LIMIT 100");

		  // Iterate over results
		  while (rset.next()) {
		    // Print out type as a string
		    System.out.println(rset.getString("type"));
		  }

		  // Clean up
		  rset.close();
		} catch (SQLException e) {
		  System.err.println("SQL Error - " + e.getMessage());
		} finally {
		  stmt.close();
		}

		Map<String, Object> params = new HashMap<>();
        params.put("dbtype", "sparql");
        params.put("database", "sparql");
        params.put("user", "anonymous");
        params.put("password", "anonymous");
        params.put("host", "http://query.wikidata.org");
        params.put("port", null);
        //params.put("database", "geotools");
		SPARQLDataStoreFactory fac=new SPARQLDataStoreFactory();
		JDBCDataStore ds = fac.createDataStore(params);
		System.out.println(ds.getDataSource().getConnection());
		//System.out.println(ds.getFeatureSource("test"));
		//DataStore test = fac.createNewDataStore(params);
		//test.getFeatureSource("test");
	}
	
}
