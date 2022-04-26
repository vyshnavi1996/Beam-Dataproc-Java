package org.edu.nwmissouri.vyshnavi;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;



public class MinimalPageRankVyshnavi {
   // DEFINE DOFNS
  // ==================================================================
  // You can make your pipeline assembly code less verbose by defining
  // your DoFns statically out-of-line.
  // Each DoFn<InputT, OutputT> takes previous output
  // as input of type InputT
  // and transforms it to OutputT.
  // We pass this DoFn to a ParDo in our pipeline.

  /**
   * DoFn Job1Finalizer takes KV(String, String List of outlinks) and transforms
   * the value into our custom RankedPage Value holding the page's rank and list
   * of voters.
   * 
   * The output of the Job1 Finalizer creates the initial input into our
   * iterative Job 2.
   */
  static class Job1Finalizer extends DoFn<KV<String, Iterable<String>>, KV<String, RankedPageVyshnavi>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<String>> element,
        OutputReceiver<KV<String, RankedPageVyshnavi>> receiver) {
      Integer contributorVotes = 0;
      if (element.getValue() instanceof Collection) {
        contributorVotes = ((Collection<String>) element.getValue()).size();
      }
      ArrayList<VotingPageVyshnavi> voters = new ArrayList<VotingPageVyshnavi>();
      for (String voterName : element.getValue()) {
        if (!voterName.isEmpty()) {
          voters.add(new VotingPageVyshnavi(voterName, contributorVotes));
        }
      }
      receiver.output(KV.of(element.getKey(), new RankedPageVyshnavi(element.getKey(), voters)));
    }
  }

  // JOB2 Mapper
  static class Job2Mapper extends DoFn<KV<String,RankedPageVyshnavi >, KV<String, RankedPageVyshnavi>> {
  @ProcessElement
  public void processElement(@Element KV<String, RankedPageVyshnavi> element,
      OutputReceiver<KV<String, RankedPageVyshnavi>> receiver) {

    Integer votes = 0;
     
      ArrayList<VotingPageVyshnavi> voters =  element.getValue().getVoters();

      if ( voters instanceof Collection){

    votes =((Collection<VotingPageVyshnavi>)voters).size();

    }
    for (VotingPageVyshnavi vp: voters){
      String pageName = vp.getName();
      Double pageRank = vp.getRank();
      String contributorPageName= element.getKey();
      Double contributorPageRank= element.getValue().getRank();
      VotingPageVyshnavi contributor = new VotingPageVyshnavi(contributorPageName,contributorPageRank,votes);
      ArrayList<VotingPageVyshnavi> arr = new ArrayList<VotingPageVyshnavi>();
      arr.add(contributor);
      receiver.output(KV.of(vp.getName(), new RankedPageVyshnavi(pageName,pageRank,arr)));
    }
  }
  }
  // JOB2 UPDATER
  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPageVyshnavi>>, KV<String, RankedPageVyshnavi>> {
  @ProcessElement
  public void processElement(@Element KV<String, Iterable<RankedPageVyshnavi>> element,
      OutputReceiver<KV<String, RankedPageVyshnavi>> receiver) {
    
        String thisPage = element.getKey();
        Iterable<RankedPageVyshnavi> rankedPage = element.getValue();
        Double dampfactor = 0.85;
        Double updateRank = (1.0 -dampfactor);
        ArrayList<VotingPageVyshnavi> newVoters = new ArrayList<VotingPageVyshnavi>();

      for (RankedPageVyshnavi pg:rankedPage) {
      if (pg!=null) {
        for(VotingPageVyshnavi vp :pg.getVoters()){
          newVoters.add(vp);
          updateRank +=(dampfactor)*vp.getRank()/(double)vp.getVotes();
        }
      }
    }
    receiver.output(KV.of(thisPage, new RankedPageVyshnavi(thisPage,updateRank,newVoters)));
  } 
}  
/**
   * Run one iteration of the Job 2 Map-Reduce process
   * Notice how the Input Type to Job 2.
   * Matches the Output Type from Job 2.
   * How important is that for an iterative process?
   * 
   * @param kvReducedPairs - takes a PCollection<KV<String, RankedPage>> with
   *                       initial ranks.
   * @return - returns a PCollection<KV<String, RankedPage>> with updated ranks.
   */
  private static PCollection<KV<String, RankedPageVyshnavi>> runJob2Iteration(
    PCollection<KV<String, RankedPageVyshnavi>> kvReducedPairs) {
    PCollection<KV<String, RankedPageVyshnavi>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));

    // KV{README.md, README.md, 1.00000, 0, [java.md, 1.00000,1]}
    // KV{README.md, README.md, 1.00000, 0, [go.md, 1.00000,1]}
    // KV{java.md, java.md, 1.00000, 0, [README.md, 1.00000,3]}

    PCollection<KV<String, Iterable<RankedPageVyshnavi>>> reducedKVs = mappedKVs
        .apply(GroupByKey.<String, RankedPageVyshnavi>create());

    // KV{java.md, [java.md, 1.00000, 0, [README.md, 1.00000,3]]}
    // KV{README.md, [README.md, 1.00000, 0, [python.md, 1.00000,1], README.md,
    // 1.00000, 0, [java.md, 1.00000,1], README.md, 1.00000, 0, [go.md, 1.00000,1]]}

    PCollection<KV<String, RankedPageVyshnavi>> updatedOutput = reducedKVs.apply(ParDo.of(new Job2Updater()));

    // KV{README.md, README.md, 2.70000, 0, [java.md, 1.00000,1, go.md, 1.00000,1,
    // python.md, 1.00000,1]}
    // KV{python.md, python.md, 0.43333, 0, [README.md, 1.00000,3]}
    return updatedOutput;
  }


   // Map to KV pairs
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

      // Group by Key to get a single record for each page
    PCollection<KV<String, Iterable<String>>> kvStringReducedPairs = myMergedList
        .apply(GroupByKey.<String, String>create());

    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPageVyshnavi>> job2in = kvStringReducedPairs.apply(ParDo.of(new Job1Finalizer()));
    PCollection<KV<String, RankedPageVyshnavi>> job2out = null; 
    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      job2out= runJob2Iteration(job2in);
      job2in =job2out;
    }
    PCollection<String> PCollectionLinksString = job2out.apply(
// Transform KV to Strings
   PCollection<String> mergeString = job2out.apply(

        MapElements.into(
            TypeDescriptors.strings())
            .via((myMergeLstout) -> myMergeLstout.toString()));
           PCollectionLinksString.apply(TextIO.write().to("VyshnaviPR"));

    p.run().waitUntilFinish();
  }
   private static PCollection<KV<String, String>> vyshnaviMapper1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;
    PCollection<String> pcolInputLines =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcolLines  = pcolInputLines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInputEmptyLines = pcolLines.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLines = pcColInputEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
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

