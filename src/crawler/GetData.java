package crawler;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import oauth.signpost.OAuthConsumer;

/**
 * 
 * @author Abhay Kawale
 * abkawale@gmail.com
 */
public class GetData extends Thread {

    /*
     * Constructor
     */
    public GetData(String topic, OAuthConsumer c, String url) throws MalformedURLException {

        this.urlString = url;
        this.topic = topic;
        this.consumer = c;
    }

    @Override
    public void run() {

        URLConnection conn; //Connection 
        BufferedReader in = null; //Reader to read XML from URL
        BufferedWriter jsonFileWrite = null; //Writer to write on to the file
        int count = 0; //Count to maintain connection retries
        boolean flag = false; //Flag to keep track of connection fails

        /*
         * Buffer to read the input data
         */
        char[] buffer = new char[4 * 1024];
        long start = System.currentTimeMillis();
        try {
            
                File file = new File(topic + "_" + System.currentTimeMillis() + ".json");
                if (!file.exists()) {
                    if (!file.createNewFile()) {
                        throw new IllegalStateException("Couldn't Create file" + file.toString());
                    }
                    jsonFileWrite = new BufferedWriter(new FileWriter(file, true));
                    uri = new URL(urlString+"?language=en&locations=-180,-90,180,90");
                    System.out.println(uri.toString());
                    conn = uri.openConnection();
                    consumer.sign(conn);
                    while (count < 10) {
                        try {
                            in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                            flag = false;
                            break;
                        } catch (Exception ex) {
                            count++;
                            flag = true;
                            Thread.sleep(5000);
                        }
                    }

                    if (flag) {
                        jsonFileWrite.write("<error>Cannot read user tweets</error>");
                        throw new IOException("Cannot read user tweets!!" + conn.getHeaderFields().toString());
                    }

                    int bytesRead = 0;
                    while ((bytesRead = in.read(buffer)) != -1) {
                        jsonFileWrite.write(buffer, 0, bytesRead);
                        Thread.sleep(500);
                    }

                    in.close();
                    jsonFileWrite.close();
                }
            
        } catch (Exception ex) {
            System.out.println("inside exception for get tweets  :-" + ex.getMessage());
        } finally {
            long finish = System.currentTimeMillis();
            System.out.println("Tweets for topic " + topic + " fetched in " + (finish - start) / 1000 + " seconds");
            
        }
    }
    /**
     * private variables
     */
    private String urlString;
    private URL uri;
    private String topic;
    private OAuthConsumer consumer;
}