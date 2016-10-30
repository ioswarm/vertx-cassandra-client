package ioswarm.vertx.ext.cassandra;

import java.util.ArrayList;
import java.util.List;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

public class ResultSet {
	
	private List<JsonObject> columns = new ArrayList<JsonObject>();
	private List<JsonObject> rows = new ArrayList<JsonObject>();
	private List<JsonArray> results;
	private boolean dirty = true;
	
	public ResultSet() {}
	
	public ResultSet(List<JsonObject> columns, List<JsonObject> rows) {
		this.columns = columns;
		this.rows = rows;
	}
	
	protected boolean isDirty() {
		return dirty;
	}
	protected void setDirty(boolean dirty) {
		this.dirty = dirty;
	}
	
	public List<JsonObject> getColumns() {
		return columns;
	}
	public ResultSet setColumns(List<JsonObject> columns) {
		this.columns = columns;
		setDirty(true);
		return this;
	}
	public ResultSet addColumn(JsonObject column) {
		getColumns().add(column);
		setDirty(true);
		return this;
	}
	
	public List<JsonObject> getRows() { 
		return rows;
	}
	public ResultSet setRows(List<JsonObject> rows) {
		this.rows = rows;
		setDirty(true);
		return this;
	}
	public ResultSet addRow(JsonObject row) {
		getRows().add(row);
		setDirty(true);
		return this;
	}
	
	public List<JsonArray> getResults() {
		if (results == null || isDirty()) {
			results = new ArrayList<JsonArray>();
			for (JsonObject row : getRows()) {
				JsonArray res = new JsonArray();
				for (int i=0;i<getColumns().size();i++) {
					JsonObject col = getColumns().get(i);
					res.add(row.getValue(col.getString("name")));
				}
				results.add(res);
			}
			setDirty(false);
		}
		return results;
	}
	
}
