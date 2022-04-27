package edu.nwmissouri.bigdatapy.manoj;
import java.util.ArrayList;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms


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


// created a class name MinimalPageRanknuvvala
public class MinimalPageRanknuvvala {

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

  static class Job2Mapper extends DoFn<KV<String, RankedPage>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, RankedPage> element,
      OutputReceiver<KV<String, RankedPage>> receiver) {
      int vote = 0;
      ArrayList<VotingPage> voters = element.getValue().getVoterList();
      if(voters instanceof Collection){
        vote = ((Collection<VotingPage>) voters).size();
      }
      for(VotingPage vp: voters){
        String pageName = vp.getVoter();
        double pageRank = vp.getPageRank();
        String cPageName = element.getKey();
        double cPageRank = element.getValue().getRank();
        VotingPage contributor = new VotingPage(cPageName,vote,cPageRank);
        ArrayList<VotingPage> array = new ArrayList<>();
        array.add(contributor);
        receiver.output(KV.of(vp.getVoter(), new RankedPage(pageName, pageRank, array)));        
      }
    }
  }

  static class Job2Updater extends DoFn<KV<String, Iterable<RankedPage>>, KV<String, RankedPage>> {
    @ProcessElement
    public void processElement(@Element KV<String, Iterable<RankedPage>> value,
      OutputReceiver<KV<String, RankedPage>> receiver) {
        Double dampingFactor = 0.85;
        Double updatedRank = (1 - dampingFactor);
        ArrayList<VotingPage> newVoters = new ArrayList<>();
        for(RankedPage rankPage:value.getValue()){
          if (rankPage != null) {
            for(VotingPage votingPage:rankPage.getVoterList()){
              newVoters.add(votingPage);
              updatedRank += (dampingFactor) * votingPage.getPageRank() / (double)votingPage.getVotes();
              // newVoters.add(new VotingPageReddy(votingPage.getVoterName(),votingPage.getContributorVotes(),updatedRank));
            }
          }
        }
        receiver.output(KV.of(value.getKey(),new RankedPage(value.getKey(), updatedRank, newVoters)));

    }

  }


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
    PCollection<KV<String, Iterable<String>>> pCollectionGroupByKey = mergedList.apply(GroupByKey.create());
    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job02Input = pCollectionGroupByKey.apply(ParDo.of(new Job1Finalizer()));
  
    PCollection<KV<String,RankedPage>> job2Mapper = job02Input.apply(ParDo.of(new Job2Mapper()));
  

  PCollection<KV<String, RankedPage>> job02Output = null; 
  PCollection<KV<String,Iterable<RankedPage>>> job02MapperGroupbkey = job2Mapper.apply(GroupByKey.create());
    
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));

  
  job02MapperGroupbkey = job02Output.apply(GroupByKey.create());
    
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));
  
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));
  job02MapperGroupbkey = job02Output.apply(GroupByKey.create());    
  job02Output = job02MapperGroupbkey.apply(ParDo.of(new Job2Updater()));   
   
  PCollection<String> pCollectionStringLists = job02Output.apply(
    MapElements.into(
        TypeDescriptors.strings()).via(
            kvtoString -> kvtoString.toString()));
   

    // generating the output with the file name Nuvvalacounts
    pCollectionStringLists.apply(TextIO.write().to("Nuvvalacounts"));  
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
