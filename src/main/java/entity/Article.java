package entity;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;

@Entity
public class Article {
    @Id
    private String link;
    @Index
    private String title;
    @Index
    private String content;
    @Index
    private String author;
    @Index
    private String source;
    @Index
    private long createdAtMLS;
    @Index
    private long updatedAtMLS;
    @Index
    private long deletedAtMLS;
    @Index
    private long publishedAtMLS;
    @Index
    private int status;

    public enum Status {
        PENDING(0), INDEXED(1), DELETED(-1);

        int status;

        Status(int status) {
            this.status = status;
        }

        public int getStatus() {
            return status;
        }
    }

    public Article() {
    }

    public Article(String link, String title, String content) {
        this.link = link;
        this.title = title;
        this.content = content;
    }

    public String getLink() {
        return link;
    }

    public void setLink(String link) {
        this.link = link;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public String getAuthor() {
        return author;
    }

    public void setAuthor(String author) {
        this.author = author;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public long getCreatedAtMLS() {
        return createdAtMLS;
    }

    public void setCreatedAtMLS(long createdAtMLS) {
        this.createdAtMLS = createdAtMLS;
    }

    public long getUpdatedAtMLS() {
        return updatedAtMLS;
    }

    public void setUpdatedAtMLS(long updatedAtMLS) {
        this.updatedAtMLS = updatedAtMLS;
    }

    public long getDeletedAtMLS() {
        return deletedAtMLS;
    }

    public void setDeletedAtMLS(long deletedAtMLS) {
        this.deletedAtMLS = deletedAtMLS;
    }

    public long getPublishedAtMLS() {
        return publishedAtMLS;
    }

    public void setPublishedAtMLS(long publishedAtMLS) {
        this.publishedAtMLS = publishedAtMLS;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
