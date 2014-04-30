/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Parser;

import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Abhay Kawale
 * abkawale@gmail.com
 */
public class ParseTweets {

    public static void main(String[] args) {
        ParseTweets pt = new ParseTweets();
        pt.worker();
    }
    
    public void worker(){
        ExecutorService executor = Executors.newFixedThreadPool(NTHREDS);
        String fileName;
        File folder = new File("/Users/Hadoop/Downloads/Twitter Data Backup");
        String csvFolder = "/Users/Hadoop/NetBeansProjects/Crawler/Processed";
        File[] listOfFiles = folder.listFiles();

        for (int i = 0; i < listOfFiles.length; i++) {
               fileName = listOfFiles[i].getPath();
               if (fileName.contains("json")){
                   fileList.add(fileName);
               }
        }
        
        for(String filename : fileList){
            String[] filenameSplit_1 = filename.split("\\.");
            String[] filenameSplit_2 = filename.split("\\/");
            String file = filenameSplit_2[filenameSplit_2.length -1];
            
            
            System.out.println (filename+","+ 
                    "/Users/Hadoop/NetBeansProjects/Crawler/src/Parser/reference"+","+ 
                    csvFolder+"/"+file+".csv");
            
            Runnable r = new Parser(filename, 
                    "/Users/Hadoop/NetBeansProjects/Crawler/src/Parser/reference", 
                    csvFolder+"/"+file+".csv");
            executor.submit(r);
        }
        executor.shutdown();
        
    }
    
    /**
     * private variables
     */
    private static final int NTHREDS = 1000;
    private ArrayList<String> fileList = new ArrayList<String>();
}
