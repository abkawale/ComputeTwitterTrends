package Parser;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import java.io.*;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * How to invoke this code: - java -jar JsonToCSV jsonFileName referenceFileName
 * CSVFilename Converts JSON to CSV!
 *
 * @author Abhay Kawale
 */
public class Parser implements Runnable{
    
    public Parser(String jsonFile, String reference, String csvFile){
        this.json = jsonFile;
        this.reference = reference;
        this.csv = csvFile;
    }
    
    @Override
    public void run() {
        
        long start = System.currentTimeMillis();
        convert(json, reference, csv);
        long end = System.currentTimeMillis();

        System.out.println("Time taken to parse "+this.json+" = " + (end - start) / 1000 +" seconds");
    }


    /**
     * Uses Jackson API to parse json file and then writes to CSV!
     *
     * @param jsonFileName
     * @param referenceFileName
     * @param CSVFilename
     */
    public void convert(String jsonFileName, String referenceFileName, String CSVFilename) {

        /**
         * create instance of the parser and fileReaders and Writers
         */
        JsonFactory f = new MappingJsonFactory();
        JsonParser jp = null;
        FileWriter fw = null;
        BufferedWriter bw = null;
        InputStream is = null;
        InputStreamReader isr = null;
        BufferedReader br = null;
        //System.out.println(CSVFilename);
        try {
            jp = f.createJsonParser(new File(jsonFileName));
            fw = new FileWriter(CSVFilename);
            bw = new BufferedWriter(fw);
            is = new FileInputStream(referenceFileName);
            isr = new InputStreamReader(is);
            br = new BufferedReader(isr);
            String line;
            while ((line = br.readLine()) != null) {
                referenceElements.add(line);
            }

            /**
             * parse
             */
            String lineToWrite = ""; // line to write in CSV
            int numElementsInLine = 0; // number of words in a line. 
            JsonToken token = jp.nextToken();
            while (token.toString() != "0xff") {


                //System.out.println(token.asString());
                String value, currentName;
                currentName = jp.getCurrentName();
                //System.out.println(currentName);

                if (currentName != null && currentName.compareToIgnoreCase("source") == 0) {

                    while (currentName.compareToIgnoreCase("filter_level")!=0) {   
                        jp.nextToken();
                        currentName = jp.getCurrentName();
                        while (currentName == null){
                            jp.nextToken();
                            currentName = jp.getCurrentName();
                        }
                    }
                }

                /**
                 * Check if the current name is not null and is present in
                 * reference file
                 */
                if (currentName != null && referenceElements.contains(currentName)) {

                    jp.nextToken();
                    value = jp.getText();
                    //System.out.println("value : - " + value);
                    /**
                     * Check if all the elements in the reference file are taken
                     * care of.. if yes then write the lineToWrite to CSV if no,
                     * then continue parsing
                     */
                    if (currentName.compareToIgnoreCase("text") == 0) {
                        value = value.replace("\n", " ").replace("\r", " ");
                    }



                    int size = referenceElements.size() - 1;
                    //System.out.println(size);
                    if (numElementsInLine == size) {
                        lineToWrite += value;
                        bw.append(lineToWrite);
                        bw.append("\n");
                        //bw.newLine();
                        numElementsInLine = 0;
                        //System.out.println(lineToWrite);
                        lineToWrite = "";
                    } else {
                        lineToWrite += value + ",";
                        numElementsInLine++;
                    }
                }
                token = jp.nextToken();
                if (token == JsonToken.END_ARRAY) {
                    token = jp.nextToken();
                }
            }

        } catch (Exception ex) {
            //System.out.println("EOF reached!");
        } finally {
            try {

                /**
                 * Close all readers and writers
                 */
                if (jp != null) {
                    jp.close();
                }
                if (bw != null) {
                    bw.close();
                }
                if (fw != null) {
                    fw.close();
                }
                if (br != null) {
                    br.close();
                }
                if (is != null) {
                    is.close();
                }
                if (isr != null) {
                    isr.close();
                }
            } catch (IOException ex) {
                Logger.getLogger(Parser.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
    }
    /**
     * private variables
     */
    private ArrayList<String> referenceElements = new ArrayList<String>(); // Used to keep track of element to parse!
    private String json;
    private String reference;
    private String csv;
}
