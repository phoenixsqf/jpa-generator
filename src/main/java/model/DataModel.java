package model;

import entity.Columns;
import entity.ConfigProperties;
import lombok.Getter;
import lombok.Setter;
import util.DataTypeUtil;
import util.StringUtil;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class DataModel {
    private String packageName;
    private String tableName;
    private String className;
    private List<Attr> attrs;

    public DataModel(ConfigProperties properties, List<Columns> columnsList) {
        String tableName = properties.getTableName();
        this.packageName = properties.getPackageName();
        this.tableName = tableName;
        this.className = StringUtil.underlineToClassName(tableName);
        this.attrs = convert(columnsList);
    }

    private List<Attr> convert(List<Columns> columnsList) {
        List<Attr> attrs = new ArrayList<>();
        columnsList.forEach(columns -> {
            Attr attr = new Attr();
            attr.setType(DataTypeUtil.getJavaType(columns.getDATA_TYPE()));
            attr.setName(StringUtil.underlineToCamel(columns.getCOLUMN_NAME()));
            attr.setIsPrimary("PRI".equals(columns.getCOLUMN_KEY()));
            attr.setOriginalName(columns.getCOLUMN_NAME());
            attrs.add(attr);
        });
        return attrs;
    }
}
