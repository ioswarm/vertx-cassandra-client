package ioswarm.vertx.ext.cassandra;

import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class CassandraTest extends CassandraTestBase {

//	@Test
//	public void testConnectionToLocalhost() throws Exception {
//		CassandraClient client = CassandraClient.createShared(vertx, new JsonObject());
//		try {
//			assertNotNull(client.cluster());
//			assertNotNull(client.session());
//			client.execute("create table user (username text, birthdate timestamp, primary key (username))", handler -> {
//				 if (handler.failed()) {
//					 System.err.println("ERROR at create table");
//					 handler.cause().printStackTrace();
//				 }
//				 
//				 client.execute("insert into user (username) values ('andreas')", handler2 -> {
//						if (handler2.failed()) {
//							System.err.println("ERROR at insert into");
//							handler2.cause().printStackTrace();
//						}
//						
//						client.query("select * from user", handler3 -> {
//							if (handler3.succeeded()) {
//								for (JsonObject jo : handler3.result()) {
//									System.out.println(jo.toString());
//								}
//									
//							} else {
//								System.err.println("ERROR at select");
//								handler3.cause().printStackTrace();
//							}
//							testComplete();
//						});
//						
//					});
//				 
//			});	
//			
//			await();
//		} finally {
//			client.close();
//		}
//		
//	}
	
}
