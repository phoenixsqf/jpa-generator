package model;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Attr {
    private Boolean isPrimary;
    private String type;
    private String name;
    private String originalName;
}
