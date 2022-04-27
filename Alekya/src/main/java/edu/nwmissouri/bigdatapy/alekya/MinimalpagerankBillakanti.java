/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.nwmissouri.bigdatapy.alekya;

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

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class MinimalpagerankBillakanti {


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

    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
    // or FlinkRunner.
    // CHANGE 2/3: Specify runner-required options.
    // For BlockingDataflowRunner, set project and temp location as follows:
    //   DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    //   dataflowOptions.setRunner(BlockingDataflowRunner.class);
    //   dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
    //   dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
    // for more details.
    //   options.as(FlinkPipelineOptions.class)
    //      .setRunner(FlinkRunner.class);

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).

    // This example reads from a public dataset containing the text of King Lear.
    //
    // DC: We don't need king lear....
    // We want to read from a folder - assign to a variable since it may change.
    // We want to read from a file - just one - we need the file name - assign to a variable. 

    String dataFolder = "web04";
    // String dataFile = "go.md";
   //  String dataPath = dataFolder + "/" + dataFile;
    

    PCollection<KV<String, String>> pcollectionkeyvaluepair1 = alekyaMapper1(p,"go.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkeyvaluepair2 = alekyaMapper1(p,"java.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkeyvaluepair3 = alekyaMapper1(p,"python.md",dataFolder);
    PCollection<KV<String, String>> pcollectionkeyvaluepair4 = alekyaMapper1(p,"README.md",dataFolder);
 
   PCollectionList<KV<String, String>> pcCollectionKVpairs = 
       PCollectionList.of(pcollectionkeyvaluepair1).and(pcollectionkeyvaluepair2).and(pcollectionkeyvaluepair3).and(pcollectionkeyvaluepair4);

    PCollection<KV<String, String>> billakantiMergedList = pcCollectionKVpairs.apply(Flatten.<KV<String,String>>pCollections());

   

    PCollection<KV<String, Iterable<String>>> pCollectionGroupByKey = billakantiMergedList.apply(GroupByKey.create());
    // Convert to a custom Value object (RankedPage) in preparation for Job 2
    PCollection<KV<String, RankedPage>> job02Ip = pCollectionGroupByKey.apply(ParDo.of(new Job1Finalizer()));
  
    PCollection<KV<String,RankedPage>> job02Mapper = job02Ip.apply(ParDo.of(new Job2Mapper()));
  

  PCollection<KV<String, RankedPage>> job02Op = null; 
  PCollection<KV<String,Iterable<RankedPage>>> job02MapperGroupbykey = job02Mapper.apply(GroupByKey.create());
    
  job02Op = job02MapperGroupbykey.apply(ParDo.of(new Job2Updater()));

  
  job02MapperGroupbykey = job02Op.apply(GroupByKey.create());
    
  job02Op = job02MapperGroupbykey.apply(ParDo.of(new Job2Updater()));
  
  job02Op = job02MapperGroupbykey.apply(ParDo.of(new Job2Updater()));
  job02MapperGroupbykey = job02Op.apply(GroupByKey.create());    
  job02Op = job02MapperGroupbykey.apply(ParDo.of(new Job2Updater()));   
    
   
   

    // Change the KV pairs to String using toString of kv
    PCollection<String> pColStringLists = job02Op.apply(
        MapElements.into(
            TypeDescriptors.strings()).via(
                kvtoString -> kvtoString.toString()));
    // Write the output to the file


        
        pColStringLists.apply(TextIO.write().to("pagerank-alekya"));
       

        p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> alekyaMapper1(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;

    PCollection<String> pcolInputLines =  p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcollection  =pcolInputLines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pColInputEmptyLines=pcollection.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLines=pColInputEmptyLines.apply(Filter.by((String line) -> line.startsWith("[")));
   
    PCollection<String> pcolIpLinks=pcolInputLinkLines.apply(
            MapElements.into(TypeDescriptors.strings())
                .via((String linkline) -> linkline.substring(linkline.indexOf("(")+1,linkline.indexOf(")")) ));

                PCollection<KV<String, String>> pcolkvpairs=pcolIpLinks.apply(
                  MapElements.into(  
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
                      .via (linkline ->  KV.of(dataFile , linkline) ));
     
                   
    return pcolkvpairs;
  }
}


    