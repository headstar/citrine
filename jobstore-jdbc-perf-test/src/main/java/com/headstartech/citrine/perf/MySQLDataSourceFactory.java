package com.headstartech.citrine.perf;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import javax.sql.DataSource;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Per Johansson
 */
public class MySQLDataSourceFactory {

    private static Properties properties;

    private MySQLDataSourceFactory() {}

    public static DataSource createDataSource() {
        HikariConfig config = new HikariConfig(loadProperties());
        HikariDataSource ds = new HikariDataSource(config);
        ds.setMaximumPoolSize(20);
        return ds;
    }

    public static List<DataSource> createDataSources(int num) {
        List<DataSource> res = new ArrayList<DataSource>();
        for(int i=0; i<num; ++i) {
            res.add(createDataSource());
        }
        return res;
    }

    public static void close(DataSource dataSource) {
        if(dataSource instanceof Closeable) {
            try {
                ((Closeable) dataSource).close();
            } catch (IOException e) {
            }
        }
    }

    private static Properties loadProperties() {
        if(properties == null) {
            try {
                properties = PropertiesLoaderUtils.loadAllProperties("mysql/mysql-jdbc.properties");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return properties;
    }

}
