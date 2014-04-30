package TermFrequencyFinder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.FieldInfo.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class CreateIndex {

    public static void main(String[] args) {

        CreateIndex cI = new CreateIndex();
        cI.index(args[0], args[1]);

    }

    /**
     * Creates an Index of document texts from Input Directory into output
     * Directory
     *
     * @param inputPath
     * @param outputPath
     */
    public void index(String inputPath, String outputPath) {
        try {

            System.out.println("Inside index method of CreateIndex..");



            File inputDirectory = new File(inputPath);
            File outputDirectory = new File(outputPath);

            Directory dir = FSDirectory.open(outputDirectory);
            Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);


            IndexWriter writer = new IndexWriter(dir,
                    new IndexWriterConfig(Version.LUCENE_CURRENT,
                    analyzer));

            for (String file : inputDirectory.list()) {
                if (file.contains("csv")) {
                    System.out.println("Now Processing " + inputPath + "/" + file);
                    BufferedReader br = new BufferedReader(new FileReader(inputPath + "/" + file));
                    String line = "";
                    while ((line = br.readLine()) != null) {
                        String[] lineContents = line.split(",");
                        //System.out.println("aloha" + lineContents.toString());
                        if (lineContents.length > 1) {
                            String[] tweetSplit = lineContents[1].split(" ");
                            for (int i = 0; i < tweetSplit.length; i++) {
                                addDoc(writer, tweetSplit[i]);
                            }
                        }
                    }
                }
            }
            writer.close();
        } catch (Exception ex) {
            System.out.println("Ouch..." + ex.getMessage());
            Logger.getLogger(CreateIndex.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private static void addDoc(IndexWriter writer, String content) throws IOException {
        FieldType fieldType = new FieldType();
        fieldType.setStoreTermVectors(true);
        fieldType.setStoreTermVectorPositions(true);
        fieldType.setIndexed(true);
        fieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        fieldType.setStored(true);
        Document doc = new Document();
        doc.add(new Field("content", content, fieldType));
        writer.addDocument(doc);
    }
}
