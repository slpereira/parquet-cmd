package  com.silvio.log.model;

import io.quarkus.hibernate.orm.panache.PanacheEntity;
import io.quarkus.hibernate.orm.panache.PanacheEntityBase;

import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "TB_COLUMN_STATISTICS")
//@IdClass(StatisticsId.class)
public class Statistics extends PanacheEntityBase implements Serializable {

    public static final long serialVersionUID = 1L;
    @Id
    @Column(name = "FILENAME")
    public String filename;

    @Id
    @Column(name = "COLUMN_NAME")
    public String columnName;

    @Id
    @Column(name = "ROW_GROUP")
    public int rowGroup;

    @Column(name = "NULL_COUNT")
    public int nullCount;

    @Column(name = "MIN_LONG")
    public Long minLong;

    @Column(name = "MAX_LONG")
    public Long maxLong;

    @Column(name = "MIN_VAR")
    public String minVar;

    @Column(name = "MAX_VAR")
    public String maxVar;

    @Column(name = "MIN_DATE")
    public java.sql.Timestamp minDate;

    @Column(name = "MAX_DATE")
    public java.sql.Timestamp maxDate;

}
