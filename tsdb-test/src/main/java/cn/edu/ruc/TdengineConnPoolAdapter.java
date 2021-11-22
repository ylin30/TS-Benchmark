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
        this.ip = ip;
        this.port = port;

        HikariConfig config = new HikariConfig();
        this.url = String.format("jdbc:TAOS://%s:%s", ip, port);
        config.setJdbcUrl(this.url);
        config.setUsername("root");
        config.setPassword("taosdata");

        // connection pool configurations
        config.setMinimumIdle(10);           //minimum number of idle connection
        config.setMaximumPoolSize(10);      //maximum number of connection in the pool
        config.setConnectionTimeout(30000); //maximum wait milliseconds for get connection from pool
        config.setMaxLifetime(0);       // maximum life time for each connection
        config.setIdleTimeout(0);       // max idle time for recycle idle connection
        config.setConnectionTestQuery("select server_status()"); //validation query

        this.ds = new HikariDataSource(config); //create datasource;

        // 创建数据库
        Connection connection = null;
        try {
            connection = getConnection();
            Statement stm = connection.createStatement();
            stm.executeUpdate("create database if not exists test");
            stm.executeUpdate("use test");
            // 创建超级表
            stm.executeUpdate("create stable metrics (time timestamp, value float) TAGS (farm nchar(6), device nchar(6), s nchar(4))");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
        return;
    }

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