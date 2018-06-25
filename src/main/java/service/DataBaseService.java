package service;

import common.ConfigException;
import entity.Columns;
import entity.ConfigProperties;
import util.ConfigUtil;

import java.sql.*;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class DataBaseService {

    public List<Columns> loadColumnsInfo(ConfigProperties configProperties) throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            Class.forName(configProperties.getDriverClassName());
            connection = DriverManager.getConnection(configProperties.getUrl(), configProperties.getUsername(), configProperties.getPassword());
            statement = connection.createStatement();
            String sql = new StringBuilder("select * from information_schema.columns where table_schema=\"")
                    .append(configProperties.getSchemaName())
                    .append("\" and table_name=\"")
                    .append(configProperties.getTableName())
                    .append("\"")
                    .toString();
            resultSet = statement.executeQuery(sql);
            List<Columns> columnsList = new ArrayList<>();
            while (resultSet.next()) {
                columnsList.add(new Columns(resultSet.getString("TABLE_SCHEMA")
                        , resultSet.getString("TABLE_NAME")
                        , resultSet.getString("COLUMN_NAME")
                        , resultSet.getInt("ORDINAL_POSITION")
                        , resultSet.getString("DATA_TYPE")
                        , resultSet.getString("COLUMN_KEY")));
            }
            return columnsList
                    .stream()
                    .sorted(Comparator.comparing(Columns::getORDINAL_POSITION))
                    .collect(toList());
        } catch (SQLException var1) {
            throw new ConfigException(var1.getMessage());
        } finally {
            if (connection != null) {
                connection.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }
}
