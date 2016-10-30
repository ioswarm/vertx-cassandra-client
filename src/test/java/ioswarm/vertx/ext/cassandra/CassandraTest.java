package ioswarm.vertx.ext.cassandra;

import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public class CassandraTest extends CassandraTestBase {

	@Test
	public void testConnectionToLocalhost() throws Exception {
		CassandraClient client = CassandraClient.createShared(vertx, new JsonObject());
		try {
			assertNotNull(client.cluster());
			assertNotNull(client.session());
			client.execute("create table user (username text, birthdate timestamp, primary key (username))", handler -> {
				 if (handler.failed()) {
					 System.err.println("ERROR at create table");
					 handler.cause().printStackTrace();
				 }
				 testComplete();
			});	
//			client.execute("insert into user (username) values ('andreas')", handler -> {
//				if (handler.failed()) {
//					System.err.println("ERROR at insert into");
//					handler.cause().printStackTrace();
//				}
//			});
			
//			client.query("select * from user", handler -> {
//				if (!handler.failed()) {
//					for (JsonObject jo : handler.result()) {
//						System.out.println(jo.toString());
//					}
//						
//				} else {
//					System.err.println("ERROR at select");
//					handler.cause().printStackTrace();
//				}
//				testComplete();
//			});
			await();
		} finally {
			client.close();
		}
		
	}
	
}
