package crawler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.util.logging.Level;
import java.util.logging.Logger;
import oauth.signpost.OAuth;
import oauth.signpost.OAuthConsumer;
import oauth.signpost.OAuthProvider;
import oauth.signpost.basic.DefaultOAuthConsumer;
import oauth.signpost.basic.DefaultOAuthProvider;

/**
 *
 * @author Abhay Kawale
 * abkawale@gmail.com
 * 
 * Use twitter streaming API to crawl tweets from all over the world on all topics.
 * 
 */
public class Crawler {

    public static void main(String args[]) {
        Crawler c = new Crawler();
        c.getData(c.getKey(), args[0]);
    }

    /**
     * @param args the command line arguments
     */
    // TODO code application logic here
    private OAuthConsumer getKey() {

        try {

            OAuthConsumer consumer = new DefaultOAuthConsumer(
                    // the consumer key of this app (replace this with yours)
                    "W2gt04t8ssUkXdM3UO6Xg",
                    // the consumer secret of this app (replace this with yours)
                    "FKJWmp9qhsmSPmVFMU2ymIn5pAMZIsrmtJXsdFJnzI");

            OAuthProvider provider = new DefaultOAuthProvider(
                    "https://api.twitter.com/oauth/request_token",
                    "https://api.twitter.com/oauth/access_token",
                    "https://api.twitter.com/oauth/authorize");

            System.out.println("Fetching request token from Twitter...");

            // we do not support callbacks, thus pass OOB
            String authUrl = provider.retrieveRequestToken(consumer, OAuth.OUT_OF_BAND);

            System.out.println("Request token: " + consumer.getToken());
            System.out.println("Token secret: " + consumer.getTokenSecret());

            System.out.println("Now visit:\n" + authUrl
                    + "\n... and grant this app authorization");
            System.out.println("Enter the PIN code and hit ENTER when you're done:");

            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String pin = br.readLine();

            System.out.println("Fetching access token from Twitter...");

            provider.retrieveAccessToken(consumer, pin);

            System.out.println("Access token: " + consumer.getToken());
            System.out.println("Token secret: " + consumer.getTokenSecret());

            return consumer;

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            return null;
        }
    }
    
    /**
     * Number of API calls to fire should be specified in for loop. 
     * @TODO : Add additional parameter to function as NumberOfAPICalls.
     * @param consumer
     * @param topic 
     */
    
    private void getData(OAuthConsumer consumer, String topic) {
        try {

            String urlstr = "https://stream.twitter.com/1.1/statuses/filter.json";
            String Topic = topic;
            for (int i = 0; i < 100; i++) {
                System.out.println("Thread # "+i);
                GetData gd = new GetData(topic, consumer, urlstr);
                gd.start();
                gd.join();
            }
        } catch (InterruptedException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (MalformedURLException ex) {
            Logger.getLogger(Crawler.class.getName()).log(Level.SEVERE, null, ex);
        }

    }
}
