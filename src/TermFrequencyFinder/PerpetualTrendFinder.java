/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package TermFrequencyFinder;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 *
 * @author Abhay Kawale abkawale@gmail.com
 * 
 * Needs a file with topics and tfidf values.
 * 
 * Standard Dev is computed using 2 pass algorithm
 * @TODO : Think of making this as a single pass algorithm. Clean code. Reuse..
 * 
 */
public class PerpetualTrendFinder {

    public static void main(String[] args) throws IOException {
        PerpetualTrendFinder ptf = new PerpetualTrendFinder();
        double mean = ptf.getMean("/Users/Hadoop/NetBeansProjects/Crawler/tfidf.txt");
        System.out.println();
        double stdDev =  ptf.computeStandardDeviation(mean,"/Users/Hadoop/NetBeansProjects/Crawler/tfidf.txt");
        ptf.perpetualTrends("/Users/Hadoop/NetBeansProjects/Crawler/tfidf.txt", mean, stdDev);
    }
    
    /**
     * 
     * A topic is a perpetual trend if its frequency is gerater than mean and variance.
     * 
     * @param fileName
     * @param mean
     * @param standardDeviation 
     */
    private void perpetualTrends(String fileName, double mean, double standardDeviation) {
        double threshold = mean + Math.pow(standardDeviation,2);
        long numOfTerms = 0;
        try {
            RandomAccessFile aFile = new RandomAccessFile(fileName, "r");
            FileChannel inChannel = aFile.getChannel();
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
            buffer.load();
            String line = "";
            for (int i = 0; i < buffer.limit(); i++) {

                char c = (char) buffer.get();

                if (c != '\n') {
                    line += c;
                } else {
                    //System.out.println(line);
                    String[] lineSplit = line.split(",");
                    if (lineSplit.length > 1) {
                        double num = Double.parseDouble(lineSplit[1]);
                        if (num > threshold && lineSplit[0].compareToIgnoreCase("gt") != 0 
                                && lineSplit[0].compareToIgnoreCase("lt")!=0) {
                            System.out.println(line);
                            numOfTerms++;
                        }
                    }
                    line = "";
                }
            }

            buffer.clear(); // do something with the data and clear/compact it.
            inChannel.close();
            aFile.close();
            System.out.println("~~~~~~~~~~~~");
            System.out.println(numOfTerms);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }

    }
    
    /**
     * Computes Standard Deviation
     * @param mean
     * @param fileName
     * @return 
     */
    private double computeStandardDeviation(double mean, String fileName) {
        double stdDeviation = 0.0;
        double StdDevSum = 0.0;
        long numOfElements = 0;
        try {
            RandomAccessFile aFile = new RandomAccessFile(fileName, "r");
            FileChannel inChannel = aFile.getChannel();
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
            buffer.load();
            String line = "";
            for (int i = 0; i < buffer.limit(); i++) {

                char c = (char) buffer.get();

                if (c != '\n') {
                    line += c;
                } else {
                    String[] lineSplit = line.split(",");
                    if (lineSplit.length > 1) {
                        double num = Double.parseDouble(lineSplit[1]);
                        double diff = Math.pow((num - mean), 2);
                        StdDevSum += diff;
                        numOfElements++;
                    }
                    line = "";
                }
            }

            buffer.clear();
            inChannel.close();
            aFile.close();

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            stdDeviation = Math.sqrt(StdDevSum / numOfElements);
            return stdDeviation;
        }
    }
    
    /**
     * Computes Mean
     * @param fileName
     * @return 
     */
    private double getMean(String fileName) {
        double mean = 0.0;
        double sum = 0.0;
        long numberOfElements = 0;
        try {
            RandomAccessFile aFile = new RandomAccessFile(fileName, "r");
            FileChannel inChannel = aFile.getChannel();
            MappedByteBuffer buffer = inChannel.map(FileChannel.MapMode.READ_ONLY, 0, inChannel.size());
            buffer.load();
            String line = "";
            for (int i = 0; i < buffer.limit(); i++) {

                char c = (char) buffer.get();

                if (c != '\n') {
                    line += c;
                } else {
                    //System.out.println(line);
                    String[] lineSplit = line.split(",");
                    if (lineSplit.length > 1) {
                        double num = Double.parseDouble(lineSplit[1]);
                        sum += num;
                        numberOfElements++;
                    }
                    line = "";
                }
            }

            buffer.clear(); // do something with the data and clear/compact it.
            inChannel.close();
            aFile.close();

            mean = sum / numberOfElements;

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {

            System.out.println("~~~~~~~~~~~~~~~");
            System.out.println("Number of elements : " + numberOfElements);
            System.out.println("Sum is : " + sum);
            System.out.println("Mean is : " + mean);
            System.out.println("~~~~~~~~~~~~~~~");
            return mean;
        }
    }
}
