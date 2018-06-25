package entity;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ConfigProperties {

    private String tableName;

    private String schemaName;

    private String driverClassName;

    private String url;

    private String username;

    private String password;

    private String packageName;

    private String output;

    private String fileName;
}
