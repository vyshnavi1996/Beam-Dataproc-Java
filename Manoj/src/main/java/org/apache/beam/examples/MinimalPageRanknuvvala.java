package org.apache.beam.examples;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

// created a class name MinimalPageRanknuvvala
public class MinimalPageRanknuvvala {
  public static void main(String[] args) {
   
    //created a PipelineOptions object
    PipelineOptions options = PipelineOptionsFactory.create();
 
    //created a PipelineOptions object that has been defined above
    Pipeline p = Pipeline.create(options);
 
    //created a datafolder where web04 file is being used
    String dataFolder = "web04";
 
   //generated a key,value pairs for each webpage
   PCollection<KV<String,String>> p1 = NuvvalaMapper01(p,"go.md",dataFolder);
   PCollection<KV<String,String>> p2 = NuvvalaMapper01(p,"python.md",dataFolder);
   PCollection<KV<String,String>> p3 = NuvvalaMapper01(p,"java.md",dataFolder);
   PCollection<KV<String,String>> p4 = NuvvalaMapper01(p,"README.md",dataFolder);
   //merged all the key value pairs in to the PCollectionList and then in to the Pcollection
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(p1).and(p2).and(p3).and(p4);
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<String> pLinksString = mergedList.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    // generating the output with the file name Nuvvalacounts
    pLinksString.apply(TextIO.write().to("Nuvvalacounts"));  
    p.run().waitUntilFinish();
  }
 public static PCollection<KV<String,String>> NuvvalaMapper01(Pipeline p, String filename, String dataFolder){
    String newdataPath = dataFolder + "/" + filename;
     PCollection<String> pcolInput = p.apply(TextIO.read().from(newdataPath));
     PCollection<String> pcollinkLines = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));
     PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.into((TypeDescriptors.strings()))
     .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));
     PCollection<KV<String,String>> pColKVPairs =  pcolLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     .via((String outLink) -> KV.of(filename,outLink)));
    return pColKVPairs;
  }

}
