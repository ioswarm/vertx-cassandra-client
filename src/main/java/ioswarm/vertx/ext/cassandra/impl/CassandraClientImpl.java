package ioswarm.vertx.ext.cassandra.impl;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import ioswarm.vertx.ext.cassandra.CassandraClient;
import ioswarm.vertx.ext.cassandra.util.CassandraUtil;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.Shareable;

public class CassandraClientImpl implements CassandraClient {

//	private static final Logger log = LoggerFactory.getLogger(CassandraClientImpl.class);
	
	private static final JsonArray EMPTY = new JsonArray(Collections.unmodifiableList(new ArrayList<>()));
	
	public static final String DS_LOCAL_MAP_NAME = "__ioswarm.vertx.CassandraClient.datasources";
	
	private final Vertx vertx;
	protected final CassandraHolder holder;
	protected Cluster cluster;
	protected Session session;
	
	public CassandraClientImpl(Vertx vertx, JsonObject config, String dataSourceName) {
		Objects.requireNonNull(vertx);
		Objects.requireNonNull(config);
		Objects.requireNonNull(dataSourceName);
		this.vertx = vertx;
		this.holder = lookupHolder(dataSourceName, config);
		this.cluster = holder.cluster();
		this.session = holder.session();
	}
	
	@Override
	public Cluster cluster() { return cluster; }
	
	@Override 
	public Session session() { return session; }
	
	@Override
	public void close() {
		holder.close();
	}
	
	@Override
	public CassandraClient execute(String cql, Handler<AsyncResult<Void>> resultHandler) {
		final ResultSetFuture resf = holder.session().executeAsync(cql);
		
		vertx.executeBlocking(handler -> {
			try {
				resf.getUninterruptibly();
				handler.complete();
			} catch(Exception e) {
				resf.cancel(true);
				handler.fail(e);
			}
		}, resultHandler);
		
		return this;
	}
	
	@Override
	public CassandraClient query(String cql, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
		return query(cql, null, resultHandler);
	}
	
	private Object[] boundaries(JsonArray params) {
		Object[] ret = new Object[params.size()];
		for (int i=0;i<params.size();i++)
			ret[i] = params.getValue(i);
		return ret;
	}
	
	@Override
	public CassandraClient query(String cql, JsonArray params, Handler<AsyncResult<List<JsonObject>>> resultHandler) {
		if (params == null) 
			params = EMPTY;
		
		PreparedStatement pstmt = holder.session().prepare(cql);
		BoundStatement bstmt = new BoundStatement(pstmt);
		final ResultSetFuture resf = holder.session().executeAsync(bstmt.bind(boundaries(params)));
		vertx.executeBlocking(handler -> {
			try {
				ResultSet res = resf.getUninterruptibly();
				List<JsonObject> l = new ArrayList<JsonObject>();
				
				ColumnDefinitions colDefs = res.getColumnDefinitions();
				for (Row row : res) {
					JsonObject jo = new JsonObject();
					for (int i=0;i<colDefs.size();i++) {
						String colName = colDefs.getName(i);
						DataType colType = colDefs.getType(i);
						
						if (!row.isNull(colName) && colType.equals(DataType.timestamp())) {
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
							jo.put(colName, sdf.format(row.getTimestamp(colName)));
						} else jo.put(colName, row.getObject(colName));
					}
					l.add(jo);
				}
				handler.complete(l);
			} catch(Exception e) {
				resf.cancel(true);
				handler.fail(e);
			}
		}, resultHandler);
		
		return this;
	}
	
	
	
	
	
	private void removeFromMap(LocalMap<String, CassandraHolder> map, String dataSourceName) {
		synchronized (vertx) {
			map.remove(dataSourceName);
			if (map.isEmpty())
				map.close();
		}
	}
	
	private CassandraHolder lookupHolder(String dataSourceName, JsonObject config) {
		synchronized (vertx) {
			LocalMap<String, CassandraHolder> map = vertx.sharedData().getLocalMap(DS_LOCAL_MAP_NAME);
			CassandraHolder theHolder = map.get(dataSourceName);
			if (theHolder == null) {
				theHolder = new CassandraHolder(config, () -> removeFromMap(map, dataSourceName));
				map.put(dataSourceName, theHolder);
			} else {
				theHolder.incRefCount();
			}
			return theHolder;
		}
	}
	
	private static class CassandraHolder implements Shareable {
		private Cluster cluster;
		private Session session;
		JsonObject config; 
		Runnable closeRunner;
		int refCount = 1;
		
		public CassandraHolder(JsonObject config, Runnable closeRunner) {
			this.config = config;
			this.closeRunner = closeRunner; 
		}
		
		public synchronized Cluster cluster() {
			if (cluster == null || cluster.isClosed()) 
				cluster = CassandraUtil.buildCluster(config);
				Session ses = cluster.connect();
				try {
					String keyspace = config.getString("keyspace", DEFAULT_KEYSPACE);
					ResultSet res = ses.execute(CassandraUtil.checkKeyspaceCommand(keyspace));
					if (res.all().size() == 0) 
						ses.execute(CassandraUtil.buildKeyspaceCommand(config, keyspace));
					
				} finally {
					ses.close();
				}
			return cluster;
		}
		
		public synchronized Session session() {
			if (session == null || session.isClosed()) {
				Cluster cluster = cluster();
				String keyspace = config.getString("keyspace", DEFAULT_KEYSPACE);
				session = cluster.connect(keyspace);
			}
			return session;
		}
		
		public synchronized void incRefCount() {
			refCount++;
		}
		
		public synchronized void close() {
			if (--refCount == 0) {
				if (cluster != null)
					cluster.close();
				if (closeRunner != null) 
					closeRunner.run();
			}
		}
		
	}
	
}
