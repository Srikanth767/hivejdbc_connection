import java.sql.Connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
class HiveJdbcTest {
	 private static Logger log = LoggerFactory
		       .getLogger(HiveJdbcTest.class);
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://34.226.212.173:10000/default");
	/*
		 ResultSet resultSet = con.getMetaData().getCatalogs();
	     while (resultSet.next()) {
	       log.info("Schema Name = " + resultSet.getString("TABLE_CAT"));
	     }
	     resultSet.close();
		
	     String[] types = { "TABLE" };
	     resultSet = con.getMetaData()
	         .getTables(databaseName, null, "%", types);
	     String tableName = "";
	     while (resultSet.next()) {
	       tableName = resultSet.getString(3);
	       log.info("Table Name = " + tableName);
	     }
	     resultSet.close();
	     DatabaseMetaData meta = con.getMetaData();
	     resultSet = meta.getColumns(databaseName, null, tableName, "%");
	     while (resultSet.next()) {
	       log.info("Column Name of table " + tableName + " = "
	           + resultSet.getString(4));
	     }
	}}
	     
	     
*/
		Statement stmt = con.createStatement();

		String tableName = "test1";
	//	stmt.executeUpdate("drop table " + tableName);
		//stmt.executeUpdate("create table " + tableName	+ " (id int, name string, dept string)");
		stmt.executeUpdate("show tables");
		// show tables
		//String sql = "select * from schema1.stable1";
		//String sql = "select * from '" + tableName + "'";
		//System.out.println("Running: " + sql);
		//ResultSet res = stmt.executeQuery(sql);
		//while (res.next()) {
			//System.out.println(res.getString(1)+" "+res.getString(2)+" "+res.getString(3));
	//	}
		//System.out.println("Connection found");
		
		for(int i=1;i<=2;i++){
			String sql = "select * from schema"+i+ ".stable"+i;
			//String sql = "select * from '" + tableName + "'";
			System.out.println("Running: " + sql);
			ResultSet res = stmt.executeQuery(sql);
			while (res.next()) {
				System.out.println(res.getString(1)+" "+res.getString(2)+" "+res.getString(3));
			}
			System.out.println("Connection found");
			
			 res.close();}
		
		
	   
		stmt.close();
		con.close();
	}
}



//print the tables
//after printing, (there is an api to get all schemas)-find it. Look for api to get all tables within the schema.
//create the data..
//look how to connect to azure sql data warehouse from hive.






//---------
//create iazure instance
//unzip and move hive
//move the req jar files for connection.
//put data in azure database(similar to s3 in aws)
//-----------