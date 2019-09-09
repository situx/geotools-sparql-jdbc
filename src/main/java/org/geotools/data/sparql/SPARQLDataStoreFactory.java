package org.geotools.data.sparql;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.geotools.data.DataStore;
import org.geotools.data.Parameter;
import org.geotools.data.jdbc.datasource.DBCPDataSource;
import org.geotools.jdbc.JDBCDataStore;
import org.geotools.jdbc.JDBCDataStoreFactory;
import org.geotools.jdbc.SQLDialect;

public class SPARQLDataStoreFactory extends JDBCDataStoreFactory {

	/** parameter for how to handle associations */
    public static final Param ASSOCIATIONS =
            new Param("Associations", Boolean.class, "Associations", false, Boolean.FALSE);
    
    public static final Param MVCC = new Param("MVCC", Boolean.class, "MVCC", false, Boolean.FALSE);
    
    public static final Param AUTO_SERVER =
            new Param(
                    "autoServer",
                    Boolean.class,
                    "Activate AUTO_SERVER mode for local file database connections",
                    false,
                    false);
	
	 public static final Param DBTYPE =
	            new Param(
	                    "dbtype",
	                    String.class,
	                    "Type",
	                    true,
	                    "sparql",
	                   Collections.singletonMap(Parameter.LEVEL, "program"));
	 
	 File baseDirectory = null;

	
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
			return new SPARQLDialect(dataStore);
		}
		
		@Override
		public DataStore createNewDataStore(Map params) throws IOException {
			// TODO Auto-generated method stub
			return super.createNewDataStore(params);
		}
		
		
		 protected JDBCDataStore createDataStoreInternal(JDBCDataStore dataStore, Map params)
		            throws IOException {
		        // check the foreign keys parameter
		        Boolean foreignKeys = (Boolean) ASSOCIATIONS.lookUp(params);

		        if (foreignKeys != null) {
		            dataStore.setAssociations(foreignKeys.booleanValue());
		        }

		        return dataStore;
		    }
		 
		 protected DataSource createDataSource(Map params, SQLDialect dialect) throws IOException {
		        BasicDataSource dataSource = new BasicDataSource();
		        dataSource.setUrl(getJDBCUrl(params));
		        System.out.println(dataSource.getUrl());
		        String username = (String) USER.lookUp(params);
		        if (username != null) {
		            dataSource.setUsername(username);
		        }
		        String password = (String) PASSWD.lookUp(params);
		        if (password != null) {
		            dataSource.setPassword(password);
		        }

		        dataSource.setDriverClassName("org.apache.jena.jdbc.JenaDriver");
		        dataSource.setPoolPreparedStatements(false);

		        return new DBCPDataSource(dataSource);
		    }
		
		 @Override
		    protected String getJDBCUrl(Map params) throws IOException {
		        String database = (String) DATABASE.lookUp(params);
		        String host = (String) HOST.lookUp(params);
		        Boolean mvcc = (Boolean) MVCC.lookUp(params);
		        Boolean autoServer = (Boolean) AUTO_SERVER.lookUp(params);
		        String autoServerSpec = Boolean.TRUE.equals(autoServer) ? ";AUTO_SERVER=TRUE" : "";

		        if (host != null && !host.equals("")) {
		            Integer port = (Integer) PORT.lookUp(params);
		            if (port != null) {
		                return "jdbc:jena:remote:" + host + ":" + port + "/" + database;
		            } else {
		                return "jdbc:jena:remote:" + host + "/" + database;
		            }
		        } else if (baseDirectory == null) {
		            // use current working directory
		            return "jdbc:jena:remote:" + database + autoServerSpec + (mvcc != null ? (";MVCC=" + mvcc) : "");
		        } else {
		            // use directory specified if the patch is relative
		            String location;
		            if (!new File(database).isAbsolute()) {
		                location = new File(baseDirectory, database).getAbsolutePath();
		            } else {
		                location = database;
		            }

		            return "jdbc:jena:remote:"
		                    + location
		                    + autoServerSpec
		                    + (mvcc != null ? (";MVCC=" + mvcc) : "");
		        }
		    }


	
}
