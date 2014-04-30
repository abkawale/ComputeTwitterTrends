package TermFrequencyFinder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.index.*;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

/**
 * Computes the Index and tfidf of terms from tweets.
 *
 * @author Abhay Kawale abkawale@gmail.com
 */
public class TFIDFcalculator {

    public static void main(String[] args) throws Exception {
        TFIDFcalculator tfidfc = new TFIDFcalculator();
        CreateIndex ci = new CreateIndex();
        ci.index("/Users/Hadoop/NetBeansProjects/Crawler/Output", "/Users/Hadoop/NetBeansProjects/Crawler/Index/");
        tfidfc.process("/Users/Hadoop/NetBeansProjects/Crawler/Index/");
    }

    /**
     * Processes each term from the Lucene index, computes tfidf of the same.
     *
     * @param indexDirectory
     * @throws Exception
     */
    private void process(String indexDirectory) throws Exception {
        try {
            System.out.println("Inside process method for TFIDFcalculator");
            File indexDir = new File(indexDirectory);
            Directory dir = FSDirectory.open(indexDir);
            DirectoryReader ireader = DirectoryReader.open(dir);
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("tfidf.txt")));
            System.out.println("Number of docs indexed by this index = " + ireader.numDocs());

            int numOfTerms = 0;
            for (int i = 0; i < ireader.numDocs(); i++) {

                Terms terms = ireader.getTermVector(i, "content");

                if (terms != null && terms.size() > 0) {
                    //System.out.println("Inside if...");
                    TermsEnum termsEnum = terms.iterator(null); // access the terms for this field
                    BytesRef term = null;
                    while ((term = termsEnum.next()) != null) {// explore the terms for this field

                        DocsEnum docsEnum = termsEnum.docs(null, null); // enumerate through documents, in this case only one
                        int docIdEnum;
                        while ((docIdEnum = docsEnum.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                            numOfTerms++;
                            double tf = docsEnum.freq();
                            double idf = getIDF(ireader, "content", term.utf8ToString());
                            double tfidf = tf * idf;
                            bw.write(term.utf8ToString() + "," + tfidf);
                            bw.newLine();
                            System.out.println(term.utf8ToString() + "," + tfidf + " ------" + numOfTerms); //get the term frequency in the document
                        }
                    }
                }
            }
            bw.close();
        } catch (IOException ex) {
            System.out.println("here.. :( ");
            Logger.getLogger(TFIDFcalculator.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Inverse Document frequency
     *
     * @param reader
     * @param field
     * @param termName
     * @return
     * @throws IOException
     */
    private double getIDF(IndexReader reader, String field, String termName) throws IOException {
        return Math.log(reader.numDocs() / ((double) reader.docFreq(new Term(field, termName))));
    }
}
