package cn.edu.ruc;

import cn.edu.ruc.utils.Pair;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class TdengineConnPoolAdapter extends TdengineAdapter2 {
    private HikariDataSource ds;

    @Override
    public Connection getConnection() {
        try {
            return this.ds.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void initConnect(String ip, String port, String user, String password) {
        // Init databae test first.
        super.initConnect(ip, port, user, password);

        // Create a connection pool specifically for database test
        HikariConfig config = new HikariConfig();
        config.setDriverClassName("com.taosdata.jdbc.TSDBDriver");
        this.url = String.format("jdbc:TAOS://%s:%s/test", ip, port);
        config.setJdbcUrl(this.url);
        config.setUsername("root");
        config.setPassword("taosdata");

        // connection pool configurations
        config.setMinimumIdle(36);           //minimum number of idle connection
        config.setMaximumPoolSize(128);      //maximum number of connection in the pool
        config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0);       // maximum life time for each connection
        config.setIdleTimeout(0);       // max idle time for recycle idle connection
        config.setConnectionTestQuery("select server_status()"); //validation query

        this.ds = new HikariDataSource(config); //create datasource;

        return;
    }

    /**
     * Override this in order to create database test before creating conn pool.
     * @param data
     * @return
     */
    @Override
    public Pair<Long, Integer> insertData(String data) {
        Connection connection = null;
        try {
            connection = getConnection();
            return insertData(data, connection);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    @Override
    public long execQuery(String sql) {
        Connection connection = null;
        try {
            connection = getConnection();
            return execQuery(sql, connection);
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }
}