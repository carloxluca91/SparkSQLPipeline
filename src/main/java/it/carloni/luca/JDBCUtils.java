package it.carloni.luca;

import org.apache.log4j.Logger;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class JDBCUtils {

    private static final Logger logger = Logger.getLogger(JDBCUtils.class);

    public static Connection getConnection(String jdbcUrl, String jdbcDriver, String jdbcUserName, String jdbcPassWord,
                                           String jdbcUseSSL) throws ClassNotFoundException, SQLException {

        Class.forName(jdbcDriver);

        String jdbcUrlConnectionStr = String.format("%s/?useSSL=%s", jdbcUrl, jdbcUseSSL);
        logger.info(String.format("Attempting to connect to JDBC url '%s' with credentials ('%s', '%s')",
                jdbcUrlConnectionStr,
                jdbcUserName,
                jdbcPassWord));

        Connection connection = DriverManager.getConnection(jdbcUrlConnectionStr, jdbcUserName, jdbcPassWord);
        logger.info(String.format("Successfully connected to JDBC url '%s' with credentials ('%s', '%s')",
                jdbcUrlConnectionStr,
                jdbcUserName,
                jdbcPassWord));

        return connection;
    }

    public static List<String> getExistingDatabases(Connection connection) throws SQLException {

        // RESULT SET CONTAINING DATABASE NAMES
        ResultSet resultSet = connection
                .getMetaData()
                .getCatalogs();

        // EXTRACT THOSE NAMES
        List<String> existingDatabases = new ArrayList<>();
        while (resultSet.next()) {

            existingDatabases.add(resultSet.getString("TABLE_CAT").toLowerCase());
        }

        return existingDatabases;
    }

    public static void createDbIfNotExists(String dbName,
                                           Connection connection,
                                           Boolean closeAfterCreation) throws SQLException {

        List<String> existingDatabases = getExistingDatabases(connection);
        boolean doesNotExistYet = existingDatabases
                .stream()
                .noneMatch(s -> s.equalsIgnoreCase(dbName));

        if (doesNotExistYet) {

            logger.info(String.format("Database '%s' does not exist yet. Thus, creating it now", dbName));
            Statement createDbStatement = connection.createStatement();
            createDbStatement.executeUpdate("CREATE DATABASE IF NOT EXISTS " + dbName.toLowerCase());
            logger.info(String.format("Successfully created database '%s'", dbName));

        } else {

            logger.warn(String.format("Database '%s' already exists. Thus, not creating it again", dbName));
        }

        if (closeAfterCreation){

            connection.close();
            logger.info("Successfully closed JDBC connection");

        } else {

            logger.warn("Not closing JDBC connection. Remember to close it manually when necessary");
        }
    }

    public static void createDbIfNotExists(String dbName,
                                           Connection connection) throws SQLException {

        createDbIfNotExists(dbName, connection, true);
    }
}
