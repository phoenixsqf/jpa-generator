package util;

import common.TypeNotFoundException;

import java.util.HashMap;
import java.util.Map;

public class DataTypeUtil {
    private static Map<String, String> typeMap = new HashMap<>();

    static {
        typeMap.put("varchar", "String");
        typeMap.put("int", "Long");
        typeMap.put("decimal", "BigDecimal");
        typeMap.put("smallint", "Integer");
        typeMap.put("tinyint", "Integer");
        typeMap.put("text", "String");
        typeMap.put("datetime", "LocalDateTime");
        typeMap.put("bigint", "BigInteger");
        typeMap.put("mediumtext", "String");
        typeMap.put("enum", "String");
    }

    public static String getJavaType(String mysqlType) {
        String javaType = typeMap.get(mysqlType);
        if (javaType == null) {
            throw new TypeNotFoundException(mysqlType);
        }
        return javaType;
    }
}
