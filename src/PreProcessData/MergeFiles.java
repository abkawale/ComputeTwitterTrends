/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package PreProcessData;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Abhay Kawale abkawale@gmail.com
 */
public class MergeFiles {

    public MergeFiles() {
        this.stopWordListMap = populateStopWordMap("/Users/Hadoop/NetBeansProjects/Crawler/src/PreProcessData/StopWords.rtf");
    }

    public static void main(String[] args) {
        MergeFiles mf = new MergeFiles();
        long start = System.currentTimeMillis();
        mf.merge("/Users/Hadoop/NetBeansProjects/Crawler/Processed/");
        long end = System.currentTimeMillis();
        System.out.println("Time to merge = " + (end - start) / 1000 + " seconds");
    }

    /**
     * Merges all files in the folder into one file
     *
     * @param folderName
     */
    private void merge(String folderName) {
        BufferedWriter bw = null;
        
        try {
            String folder = "/Users/Hadoop/NetBeansProjects/Crawler/Output/";
            File files = new File(folderName);
            File[] listOfFiles = files.listFiles();


            for (int i = 0; i < listOfFiles.length; i++) {
                fileList.add(listOfFiles[i].getPath());
            }


            for (String s : fileList) {
                BufferedReader br = new BufferedReader(new FileReader(s));
                File mergedFile = new File(folder+"TweetsData_"+System.currentTimeMillis()+".csv");
                bw = new BufferedWriter(new FileWriter(mergedFile));
                String line;
                while ((line = br.readLine()) != null) {
                    tweetCount++;
                    line = removeStopWordsFromTweet(line);
                    bw.append(line);
                    bw.newLine();
                }
                br.close();
                bw.close();
            }
        } catch (IOException ex) {
            Logger.getLogger(MergeFiles.class.getName()).log(Level.SEVERE, null, ex);
        } 
    }

    /**
     * Removes stop words from the tweet
     *
     * @param tweet
     * @return
     */
    private String removeStopWordsFromTweet(String tweet) {

        String tweetWithoutStopWords = "";

        String tweetWords[] = tweet.split(" ");

        for (String word : tweetWords) {
            if (getStopWordListMap().get(word.toLowerCase()) == null) {
                tweetWithoutStopWords += word + " ";
            }
        }
        System.out.println(tweetWithoutStopWords);
        return tweetWithoutStopWords;
    }

    /**
     * Reads the stop words file and populates the hash map of stop words
     *
     * @param stopWordList
     */
    private static HashMap<String, Integer> populateStopWordMap(String stopWordList) {

        HashMap<String, Integer> stopWordsMap = new HashMap<String, Integer>();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(stopWordList));
            String line;

            while ((line = br.readLine()) != null) {
                line = line.trim();
                line = line.replace("\\", "");
                stopWordsMap.put(line, 1);
            }
        } catch (IOException ex) {
            Logger.getLogger(MergeFiles.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException ex) {
                    Logger.getLogger(MergeFiles.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return stopWordsMap;
        }
    }

    /**
     * @return the stopWordListMap
     */
    public HashMap<String, Integer> getStopWordListMap() {
        return stopWordListMap;
    }
    /**
     * private variables
     */
    private static HashMap<String, Integer> stopWordListMap = new HashMap<String, Integer>();
    private ArrayList<String> fileList = new ArrayList<String>();
    private long tweetCount = 0;
}
