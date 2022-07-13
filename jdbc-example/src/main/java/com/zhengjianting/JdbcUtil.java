package com.zhengjianting;

import com.alibaba.druid.pool.DruidDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class JdbcUtil {

    // 声明连接池对象
    private static DataSource dataSource;

    // 1. 初始化连接池容器  static{}
    static {
        try {
            // 1.加载druid.properties 配置文件
            InputStream in = JdbcUtil.class.getClassLoader().getResourceAsStream("druid.properties");
            Properties properties = new Properties();
            properties.load(in);

            // 2.通过druid的工厂，创建连接池对象
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            throw new RuntimeException("druid连接池初始化失败...");
        }
    }

    // 2. 提供获取连接池对象的静态方法
    public static DataSource getDataSource() {
        return dataSource;
    }

    // 3. 提供获取连接对象的静态方法
    public static Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    // 4. 提供释放资源的方法（conn对象不是销毁，而是归还到连接池）
    public static void release(ResultSet resultSet, Statement statement, Connection connection) {
        // 关闭ResultSet
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        // 关闭Statement
        if (statement != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        // 关闭Connection
        if (connection != null) {
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    // 方法重载
    public static void release(Statement statement, Connection connection) {
        release(null, statement, connection);
    }
}