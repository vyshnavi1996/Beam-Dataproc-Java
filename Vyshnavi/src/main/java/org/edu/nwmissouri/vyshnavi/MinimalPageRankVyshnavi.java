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

  // JOB2 MAPPER
 static class Job2Mapper extends DoFn<KV<String,RankedPage>, KV<String, RankedPage>> {
  @ProcessElement
  public void processElement(@Element KV<String, RankedPage> element,
      OutputReceiver<KV<String, RankedPage>> receiver) {
        // set Integer votes to 0
    Integer votes = 0;
      // create ArrayList<VotingPage> named voters and set to element getValue() (a RankedPage) and call getVoters()
      ArrayList<VotingPage> voters =  element.getValue().getVoters();
      // if voters instanceof Collection then
      if ( voters instanceof Collection){
    // set votes to ((Collection<VotingPage>) voters).size()
    votes =((Collection<VotingPage>)voters).size();
    //end if
    }
    // for (VotingPage vp : voters) {
    for (VotingPage vp: voters){
      String pageName = vp.getName();
      Double pageRank = vp.getRank();
      String contributorPageName= element.getKey();
      Double contributorPageRank= element.getValue().getRank();
      VotingPage contributor = new VotingPage(contributorPageName,contributorPageRank,votes);
      ArrayList<VotingPage> arr = new ArrayList<VotingPage>();
      arr.add(contributor);
      receiver.output(KV.of(vp.getName(), new RankedPage(pageName,pageRank,arr)));
    }
  }
 

  }
  // HELPER FUNCTIONS
  public static  void deleteFiles(){
    final File file = new File("./");
    for (File f : file.listFiles()){
     if(f.getName().startsWith("VyshnaviPR")){
    f.delete();
    }
     }
   }
   // JOB2 UPDATER
 static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
  @ProcessElement
  public void processElement(@Element KV<String, Iterable<RankedPage>> element,
    OutputReceiver<KV<String, RankedPage>> receiver) {
      //Double dampingFactor = 0.85
      Double dampingFactor = 0.85;
      //Double updatedRank = (1 - dampingFactor) to start
      Double updatedRank = (1 - dampingFactor);
      //Create a  new array list for newVoters
      ArrayList<VotingPage> newVoters = new ArrayList<>();
      //For each pg in rankedPages, if pg isn't null, for each vp in pg.getVoters()
      for(RankedPage pg:element.getValue()){
        if (pg != null) {
          for(VotingPage vp:pg.getVoters()){
            newVoters.add(vp);
            updatedRank += (dampingFactor) * vp.getRank() / (double)vp.getVotes();

          }
        }
      }
      receiver.output(KV.of(element.getKey(),new RankedPage(element.getKey(), updatedRank, newVoters)));

  }
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
 private static PCollection<KV<String, RankedPage>> runJob2Iteration(
      PCollection<KV<String, RankedPage>> kvReducedPairs) {
     PCollection<KV<String, RankedPage>> mappedKVs = kvReducedPairs.apply(ParDo.of(new Job2Mapper()));
   // apply(ParDo.of(new Job2Mapper()));

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

   PCollection<KV<String, Iterable<String>>> kvReducedPairs = myMergedList.apply(GroupByKey.<String, String>create());

    // Convert to a custom Value object (RankedPageKeerthiMuli) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job2in = kvReducedPairs.apply(ParDo.of(new Job1Finalizer()));

    PCollection<KV<String, RankedPage>> job2out = null; 
    int iterations = 50;
    for (int i = 1; i <= iterations; i++) {
      job2out= runJob2Iteration(job2in);
      job2in =job2out;
    }
    // Transform KV to Strings
   PCollection<String> mergeString = job2out.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((kvInput) -> kvInput.toString()));
           mergeString.apply(TextIO.write().to("VyshnaviPR"));

    p.run().waitUntilFinish();
  }
}
