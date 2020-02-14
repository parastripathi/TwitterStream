package entity;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DataModified {
    private String id;
    private Long createdAt;
    private String text;
    private String authorId;

}
