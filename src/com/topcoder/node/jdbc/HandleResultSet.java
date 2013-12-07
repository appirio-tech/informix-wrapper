/**
 * Copyright (C) 2013 TopCoder Inc., All Rights Reserved.
 */
package com.topcoder.node.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

/**
 * A representation of a {@link ResultSet} that is suitable for marshalling in
 * to JSON.
 *
 * @author pvmagacho
 * @version 1.0
 */
public class HandleResultSet {
	/**
	 * The database connection
	 */
	private Connection connection;

	/**
	 * Constructor
	 */
    public HandleResultSet(Connection connection)  {
		this.connection = connection;
    }

    /**
     * Create a result set.
     *
     * @param resultSet
     *            the result set to extract data from
     * @throws SQLException
     *             if something went wrong extracting the data.
     */
    public String executeQuery(String sql) throws SQLException {
		List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

		Statement st = null;
		ResultSet resultSet = null;
		try {
			st = this.connection.createStatement();
			resultSet = st.executeQuery(sql);

			if (resultSet == null) {
				return "";
			}

			ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
			while (resultSet.next()) {
				Map<String, Object> row = new HashMap<String, Object>();

				for (int column = 1; column <= resultSetMetaData.getColumnCount(); column++) {
					String columnName = resultSetMetaData.getColumnName(column);
					Object columnValue = resultSet.getObject(column);

					row.put(columnName, columnValue);
				}

				rows.add(row);
			}

			return getRowsJson(rows);
		} finally {
			if (resultSet != null) resultSet.close();
			if (st != null) st.close();
		}
    }

    /**
     * Create a result set.
     *
     * @param resultSet
     *            the result set to extract data from
     * @throws SQLException
     *             if something went wrong extracting the data.
     */
    public Integer executeUpdate(String sql) throws SQLException {
    	Statement st = null;
    	Integer count = 0;
		try {
			this.connection.setAutoCommit(false);
			st = this.connection.createStatement();

			count = st.executeUpdate(sql);

			this.connection.commit();
		} catch (SQLException e) {
			this.connection.rollback();

			throw e;
		} finally {
			if (st != null) st.close();

			return count;
		}
	}

    /**
     * Gets the rows as JSON String.
     *
     * @return the JSON string representation
     */
    private String getRowsJson(List<Map<String, Object>> rows) {
        // use of iso8601 format
    	Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").create();

        return gson.toJson(rows);
    }
}
