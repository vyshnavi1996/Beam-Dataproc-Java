
package org.apache.beam.examples;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;



public class MinimalPageRankVyshnavi {

  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPage>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPage(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPage(element.getKey(), voters)));
    }
  }



  private static PCollection<KV<String, RankedPage>> runJob2Iteration(
      PCollection<KV<String, RankedPage>> kvReducedPairs) {
    PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));

    // KV{README.md, README.md, 1.00000, 0, [java.md, 1.00000,1]}
    // KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,1]}
    // KV{java.md, java.md, 1.00000, 0, [README.md, 1.00000,3]}

    PCollection<KV<String, Iterable<RankedPage>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPage>create());

    // KV{java.md, [java.md, 1.00000, 0, [README.md, 1.00000,3]]}
    // KV{README.md, [README.md, 1.00000, 0, [python.md, 1.00000,1], README.md,
    // 1.00000, 0, [java.md, 1.00000,1], README.md, 1.00000, 0, [go.md, 1.00000,1]]}

    PCollection<KV<String, RankedPage>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));

    // KV{README.md, README.md, 2.70000, 0, [java.md, 1.00000,1, go.md, 1.00000,1,
    // python.md, 1.00000,1]}
    // KV{python.md, python.md, 0.43333, 0, [README.md, 1.00000,3]}
    return updatedOutput;
  }
  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    Pipeline p = Pipeline.create(options);

    String folder="web04";

      // .apply(Filter.by((String line) -> !line.isEmpty()))    
      // .apply(Filter.by((String line) -> !line.equals(" ")))
    PCollection<KV<String, String>> pcollection1 = vyshnaviMapper1(p,"go.md",folder);
    PCollection<KV<String, String>> pcollection2 = vyshnaviMapper1(p,"java.md",folder);
    PCollection<KV<String, String>> pcollection3 = vyshnaviMapper1(p,"python.md",folder);
    PCollection<KV<String, String>> pcollection4 = vyshnaviMapper1(p,"README.md",folder);
    PCollectionList<KV<String, String>> PCollection_KV_pairs = PCollectionList.of(pcollection1).and(pcollection2).and(pcollection3).and(pcollection4);

    PCollection<KV<String, String>> myMergedList = PCollection_KV_pairs.apply(Flatten.<KV<String,String>>pCollections());

    PCollection<String> PCollectionLinksString =  myMergedList.apply(
      MapElements.into(  
        TypeDescriptors.strings())
          .via((myMergeLstout) -> myMergeLstout.toString()));


        
        PCollectionLinksString.apply(TextIO.write().to("VyshnaviPR"));

    p.run().waitUntilFinish();
  }
    private static PCollection<KV<String, String>> vyshnaviMapper1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;
    PCollection<String> pcolInputLines =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcolLines  =pcolInputLines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInputEmptyLines=pcolLines.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLines=pcColInputEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> pcolInputLinks=pcolInputLinkLines.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));

                PCollection<KV<String, String>> pcollectionkvLinks=pcolInputLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linkline ->  KV.of(dataFile , linkline) ));
     
                   
    return pcollectionkvLinks;
  }
}
