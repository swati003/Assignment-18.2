import java.io.IOException;

import javax.security.auth.login.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/* The program should be able to check for the presence of 'customer' table.
In case the table exists, it must be dropped and recreated.*/

//Author:- Swati Sahu

public class HbaseTable {
	
	
	
	org.apache.hadoop.conf.Configuration conf = HBaseConfiguration.create();
	
	public void createTable(String name, String colfamily[]) throws MasterNotRunningException,ZooKeeperConnectionException{
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			HTableDescriptor desc = new HTableDescriptor(Bytes.toBytes(name));
			
			for(int i= 0;i<colfamily.length;i++){
				desc.addFamily(new HColumnDescriptor(colfamily[i]));
				
			}
			if(admin.tableExists(name)){
				System.out.println("Table Already exists");
			}
			else{
				admin.createTable(desc);
				System.out.println("Table :" + name + "Successfully created" );
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public void deleteTable(String tableName) throws MasterNotRunningException,ZooKeeperConnectionException,IOException{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if(admin.tableExists(tableName)){
			admin.disableTable(tableName);
			admin.deleteTable(tableName);
			System.out.println("Table: " + tableName + "deleted");
		}else
		{
			System.out.println("Table doesnot exists");
		}
		
	}

	public static void main(String[] args)throws MasterNotRunningException,ZooKeeperConnectionException,IOException {
		// TODO Auto-generated method stub
		HbaseTable table = new HbaseTable();
		String tablename = "Customer";
		String[] familys = {"Name","Address"};
		table.createTable(tablename, familys);
		table.deleteTable(tablename);
		
		
	}

}
