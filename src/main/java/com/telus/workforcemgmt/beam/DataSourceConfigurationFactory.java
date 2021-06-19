package com.telus.workforcemgmt.beam;

import java.util.Properties;

import org.apache.beam.sdk.io.jdbc.JdbcIO;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class DataSourceConfigurationFactory {

	private static HikariConfig config;
	private static HikariDataSource ds;
	private static JdbcIO.DataSourceConfiguration dsConfig;

	static {
		
		  String jdbcURL = String.format("jdbc:postgresql:///%s", "ngcm");
		    Properties connProps = new Properties();
		    connProps.setProperty("user", "wfm-dbuser_dv");
		    connProps.setProperty("password", "wfm-dbuser_dv");
		    connProps.setProperty("sslmode", "disable");
		    connProps.setProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
		    connProps.setProperty("cloudSqlInstance", "cio-wfm-messaging-lab-f81efa:us-central1:fwds-instance-11d74588");
		    connProps.setProperty("enableIamAuth", "true");
		    connProps.setProperty("ipTypes", "PRIVATE");
		    // Initialize connection pool
		    HikariConfig config = new HikariConfig();
		    config.setJdbcUrl(jdbcURL);
		    config.setDataSourceProperties(connProps);
		    config.setConnectionTimeout(10000); // 10s

		    ds = new HikariDataSource(config);
		/*    
		    
		config.setJdbcUrl("jdbc:postgresql://localhost:5432/ngcm");
		config.setUsername( "wfm-dbuser_dv" );
		config.setPassword( "wfm-dbuser_dv" );
		//config.setDataSourceClassName("org.postgresql.ds.PGPoolingDataSource");
		config.addDataSourceProperty( "cachePrepStmts" , "true" );
		//config.addDataSourceProperty( "prepStmtCacheSize" , "250" );
		//config.addDataSourceProperty( "prepStmtCacheSqlLimit" , "2048" );
		config.addDataSourceProperty("socketFactory", "com.google.cloud.sql.postgres.SocketFactory");
		config.addDataSourceProperty("cloudSqlInstance", "cio-wfm-messaging-lab-f81efa:us-central1:fwds-instance-11d74588");
		config.addDataSourceProperty("ipTypes", "PRIVATE");
		
		
		config.setMaximumPoolSize(5);
		config.setMinimumIdle(1);
		ds = new HikariDataSource( config );
		*/
		dsConfig = JdbcIO.DataSourceConfiguration.create(ds);
	}
	
	public static JdbcIO.DataSourceConfiguration create () {
		return dsConfig;
	}

}
