package entity;

import lombok.Data;

@Data
public class Columns {

    private String TABLE_SCHEMA;

    private String TABLE_NAME;

    private String COLUMN_NAME;

    private Integer ORDINAL_POSITION;

    private String DATA_TYPE;

    private String COLUMN_KEY;

    public Columns() {
    }

    public Columns(String TABLE_SCHEMA, String TABLE_NAME, String COLUMN_NAME, Integer ORDINAL_POSITION, String DATA_TYPE, String COLUMN_KEY) {
        this.TABLE_SCHEMA = TABLE_SCHEMA;
        this.TABLE_NAME = TABLE_NAME;
        this.COLUMN_NAME = COLUMN_NAME;
        this.ORDINAL_POSITION = ORDINAL_POSITION;
        this.DATA_TYPE = DATA_TYPE;
        this.COLUMN_KEY = COLUMN_KEY;
    }
}
