package ioswarm.vertx.ext.cassandra.util;

import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class CassandraUtil {
	
	private static final String CREATE_KEYSPACE_COMMAND = "CREATE KEYSPACE %1$s WITH REPLICATION = {%2$s}";
	private static final String CHECK_KEYSPACE_COMMAND = "SELECT * FROM SYSTEM.SCHEMA_KEYSPACES WHERE KEYSPACE_NAME = %1$s LIMIT 1";
	
	private static final String REPLICATION_SIMPLE = "SimpleStrategy";
	private static final String REPLICATION_NETWORK_TOPOLOGY = "NetworkTopologyStrategy";
	
	public static Cluster buildCluster(JsonObject config) {
		Cluster.Builder builder = Cluster.builder();
		builder.withRetryPolicy(DefaultRetryPolicy.INSTANCE);
//		builder.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()));
		builder.withLoadBalancingPolicy(new TokenAwarePolicy(new DCAwareRoundRobinPolicy()));
		
		if (config.containsKey("hosts")) {
			JsonArray hosts = config.getJsonArray("hosts");
			for (int i=0;i<hosts.size();i++)
				builder.addContactPoint(hosts.getString(i));
		} else builder.addContactPoint(config.getString("host", "localhost"));
			
		return builder.build();
	}
	
	private static String mapToString(Map<String, String> map) {
		String ret = "";
		for (String key : map.keySet())
			ret += (ret.length()==0 ? "" : ",") + key + " : " + map.get(key);
		return ret;
	}
	
	private static String quote(String value, String quote) {
		return quote+value+quote;
	}
	
	private static String quote(String value) {
		return quote(value, "'");
	}
	
	public static String checkKeyspaceCommand(String keyspace) {
		return String.format(CHECK_KEYSPACE_COMMAND, quote(keyspace));
	}
	
	public static String buildKeyspaceCommand(JsonObject config, String keyspace) {
		Map<String, String> map = new HashMap<String, String>();
		String replicationClass = config.getString("replicationClass", REPLICATION_SIMPLE);
		if (REPLICATION_NETWORK_TOPOLOGY.equals(replicationClass) && config.containsKey("replicationDataCenter")) {
			JsonArray arr = config.getJsonArray("replicationDataCenter");
			for (int i=0;i<arr.size();i++) {
				JsonObject dc = arr.getJsonObject(i);
				map.put(quote(dc.getString("name")), String.valueOf(dc.getInteger("replicas", 2)));
			}	
		} else {
			replicationClass = REPLICATION_SIMPLE;
			map.put(quote("replication_factor"), String.valueOf(config.getInteger("replicationFactor", 3)));
		}
		map.put(quote("class"), quote(replicationClass));
		
		return String.format(CREATE_KEYSPACE_COMMAND, keyspace, mapToString(map));
	}
	
}
