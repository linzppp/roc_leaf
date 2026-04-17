package core.segment.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Entity
@Table(name = "leaf_alloc")
@Data
@NoArgsConstructor
public class LeafAlloc {

    @Id
    @Column(name = "biz_tag")
    private String bizTag;

    @Column(name = "max_id")
    private long maxId;

    private int step;

    private String description;

    @Column(name = "update_time")
    private LocalDateTime updateTime;
}
