package service;

import entity.Columns;
import entity.ConfigProperties;
import util.ConfigUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class DataBaseService {

    public List<Columns> loadColumnsInfo(ConfigProperties configProperties) throws Exception {
        Class.forName(configProperties.getDriverClassName());
        Connection connection = DriverManager.getConnection(configProperties.getUrl(), configProperties.getUsername(), configProperties.getPassword());

        Statement statement = connection.createStatement();
        String sql = new StringBuilder("select * from information_schema.columns where table_schema=\"")
                .append(configProperties.getSchemaName())
                .append("\" and table_name=\"")
                .append(configProperties.getTableName())
                .append("\"")
                .toString();
        ResultSet resultSet = statement.executeQuery(sql);
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
    }
}
