package org.geotools.data.sparql;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.geotools.data.Parameter;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;

public class SPARQLDataStoreFactory extends JDBCDataStoreFactory {

	 public static final Param DBTYPE =
	            new Param(
	                    "dbtype",
	                    String.class,
	                    "Type",
	                    true,
	                    "sparql",
	                   Collections.singletonMap(Parameter.LEVEL, "program"));
	
	 public String getDisplayName() {
	        return "SPARQL";
	    }

	    protected String getDriverClassName() {
	        return "org.apache.jena.jdbc.JenaDriver";
	    }

	    protected String getDatabaseID() {
	        return "sparql";
	    }

	    public String getDescription() {
	        return "SPARQL Endpoint";
	    }

	    @Override
	    protected String getValidationQuery() {
	        return "select version()";
	    }

		@Override
		protected SQLDialect createSQLDialect(JDBCDataStore dataStore) {
			// TODO Auto-generated method stub
			return null;
		}
		
		@Override
	    protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map params)
	            throws IOException {
	        String storageEngine = (String) STORAGE_ENGINE.lookUp(params);
	        if (storageEngine == null) {
	            storageEngine = (String) STORAGE_ENGINE.sample;
	        }

	        SQLDialect dialect = dataStore.getSQLDialect();
	        if (dialect instanceof SPARQLDialect) {
	            ((SPARQLDialect) dialect).setStorageEngine(storageEngine);
	        } else {
	            ((SPARQLDialect) dialect).setStorageEngine(storageEngine);
	        }

	        return dataStore;
	    }


	
}
