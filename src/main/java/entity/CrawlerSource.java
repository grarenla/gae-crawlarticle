package entity;

import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Index;
import com.googlecode.objectify.annotation.Unindex;

@Entity
public class CrawlerSource {
    @Id
    private String url;
    @Unindex
    private String linkSelector;
    @Unindex
    private String titleSelector;
    @Unindex
    private String contentSelector;
    @Unindex
    private String authorSelector;
    @Index
    private int status;

    public enum Status {
        DEACTIVE(0), ACTIVE(1), DELETED(-1);

        int value;

        Status(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

    }

    public CrawlerSource() {
    }

    public CrawlerSource(String url, String linkSelector, String titleSelector, String contentSelector, String authorSelector, int status) {
        this.url = url;
        this.linkSelector = linkSelector;
        this.titleSelector = titleSelector;
        this.contentSelector = contentSelector;
        this.authorSelector = authorSelector;
        this.status = status;
    }

    public CrawlerSource(String url, int status) {
        this.url = url;
        this.status = status;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getLinkSelector() {
        return linkSelector;
    }

    public void setLinkSelector(String linkSelector) {
        this.linkSelector = linkSelector;
    }

    public String getTitleSelector() {
        return titleSelector;
    }

    public void setTitleSelector(String titleSelector) {
        this.titleSelector = titleSelector;
    }

    public String getContentSelector() {
        return contentSelector;
    }

    public void setContentSelector(String contentSelector) {
        this.contentSelector = contentSelector;
    }

    public String getAuthorSelector() {
        return authorSelector;
    }

    public void setAuthorSelector(String authorSelector) {
        this.authorSelector = authorSelector;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}
