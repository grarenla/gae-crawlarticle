package crawler;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskHandle;
import com.googlecode.objectify.ObjectifyService;
import config.GAEConstant;
import entity.Article;
import entity.CrawlerSource;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static com.googlecode.objectify.ObjectifyService.ofy;

public class PullQueueArticle extends HttpServlet {
    private static final Logger LOGGER = Logger.getLogger(PullQueueArticle.class.getName());
    private Queue queue = QueueFactory.getQueue(GAEConstant.GAE_QUEUE_NAME);
    static {
        ObjectifyService.register(Article.class);
        ObjectifyService.register(CrawlerSource.class);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        List<TaskHandle> tasks = queue.leaseTasks(10, TimeUnit.SECONDS, 1);
        if (tasks.size() > 0) {
            LOGGER.warning(new String(tasks.get(0).getPayload()));
            Article article = ofy().load().type(Article.class).id(new String(tasks.get(0).getPayload())).now();
            LOGGER.warning(article.getSource());
            CrawlerSource crawlerSource = ofy().load().type(CrawlerSource.class).id(article.getSource()).now();
            Document dom = Jsoup.connect(article.getLink()).method(Connection.Method.GET).execute().parse();
            String title = dom.select(crawlerSource.getTitleSelector()).text();
            String content = dom.select(crawlerSource.getContentSelector()).html();
            article.setTitle(title);
            article.setContent(content);
            article.setUpdatedAtMLS(Calendar.getInstance().getTimeInMillis());
            article.setStatus(Article.Status.INDEXED.getStatus());
            ofy().save().entity(article).now();
            queue.deleteTask(tasks.get(0));
        }
    }
}
