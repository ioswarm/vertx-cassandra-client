package ioswarm.vertx.ext.cassandra;

import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import io.vertx.test.core.VertxTestBase;

public class CassandraTestBase extends VertxTestBase {

	 
	
	@BeforeClass
	public static void startCassandra() throws Exception {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("/cassandra.yaml");
		
	}
	
	@AfterClass
	public static void stopCassandra() throws Exception {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}
	
}
