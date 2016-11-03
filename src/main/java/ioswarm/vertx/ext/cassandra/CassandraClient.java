package ioswarm.vertx.ext.cassandra;

import java.util.List;
import java.util.UUID;

import ioswarm.vertx.ext.cassandra.impl.CassandraClientImpl;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;

import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

@VertxGen
public interface CassandraClient {

	public static final String DEFAULT_DS = "cassandra.default.ds";
	
	public static final String DEFAULT_KEYSPACE = "default";
	
	public static CassandraClient createNonShared(Vertx vertx, JsonObject config) {
		return new CassandraClientImpl(vertx, config, UUID.randomUUID().toString());
	}
	
	public static CassandraClient createShared(Vertx vertx, JsonObject config, String dataSourceName) {
		return new CassandraClientImpl(vertx, config, dataSourceName);
	}
	
	public static CassandraClient createShared(Vertx vertx, JsonObject config) {
		return createShared(vertx, config, DEFAULT_DS);
	}
	
	public Cluster cluster();
	public Session session();
	
	public CassandraClient execute(Statement stmt, Handler<AsyncResult<com.datastax.driver.core.ResultSet>> resultSetHandler);
	
	public CassandraClient execute(String cql, Handler<AsyncResult<Void>> resultHandler);
	public CassandraClient execute(String cql, JsonArray params, Handler<AsyncResult<Void>> resultHandler);	
	
	public CassandraClient query(String cql, Handler<AsyncResult<List<JsonObject>>> resultHandler);
	public CassandraClient query(String cql, JsonArray params, Handler<AsyncResult<List<JsonObject>>> resultHandler);
	
	public void close();
	
}
