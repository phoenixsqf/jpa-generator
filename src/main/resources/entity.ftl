package ${packageName};

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.DynamicInsert;
import org.hibernate.annotations.GenericGenerator;

import javax.persistence.*;
import java.math.BigDecimal;
import java.util.List;

@Entity
@Table(name = "${tableName}")
@Getter
@Setter
@JsonIgnoreProperties(value = { "handler","hibernateLazyInitializer" }, ignoreUnknown = true)
@DynamicInsert
public class ${className} {
    <#list attrs as attr>
    <#if attr.isPrimary == true>
    @Id
    </#if>
    @Column(name = "${attr.originalName}")
    private ${attr.type} ${attr.name};

    </#list>

}