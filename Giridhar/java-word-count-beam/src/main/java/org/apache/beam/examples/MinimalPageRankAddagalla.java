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
package org.apache.beam.examples;

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

/**
 * An example that counts words in Shakespeare.
 *
 * <p>
 * This class, {@link MinimalWordCount}, is the first in a series of four
 * successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any
 * error-checking or
 * argument processing, and focus on construction of the pipeline, which chains
 * together the
 * application of core transforms.
 *
 * <p>
 * Next, see the {@link WordCount} pipeline, then the
 * {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce
 * additional
 * concepts.
 *
 * <p>
 * Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>
 * No arguments are required to run this pipeline. It will be executed with the
 * DirectRunner. You
 * can see the results in the output files in your current working directory,
 * with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would
 * use an appropriate
 * file service.
 */
public class MinimalPageRankAddagalla {

  public static void main(String[] args) {

    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific
    // changes:
    //
    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
    // or FlinkRunner.
    // CHANGE 2/3: Specify runner-required options.
    // For BlockingDataflowRunner, set project and temp location as follows:
    // DataflowPipelineOptions dataflowOptions =
    // options.as(DataflowPipelineOptions.class);
    // dataflowOptions.setRunner(BlockingDataflowRunner.class);
    // dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
    // dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
    // for more details.
    // options.as(FlinkPipelineOptions.class)
    // .setRunner(FlinkRunner.class);

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read
    // to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is
    // one line from
    // the input text (a set of Shakespeare's texts).

    // This example reads from a public dataset containing the text of King Lear.
    String datafolder = "web04";
    // This example reads from a public dataset containing the text of King Lear.
    // .apply(Filter.by((String line) -> !line.isEmpty()))
    // .apply(Filter.by((String line) -> !line.equals(" ")))
    PCollection<KV<String, String>> pcollection_kv_1 = GiridharMapper01(p, "go.md", datafolder);
    PCollection<KV<String, String>> pcollection_kv_2 = GiridharMapper01(p, "java.md", datafolder);
    PCollection<KV<String, String>> pcollection_kv_3 = GiridharMapper01(p, "python.md", datafolder);
    PCollection<KV<String, String>> pcollection_kv_4 = GiridharMapper01(p, "README.md", datafolder);
    PCollectionList<KV<String, String>> PCollection_KV_pairs = PCollectionList.of(pcollection_kv_1)
        .and(pcollection_kv_2).and(pcollection_kv_3).and(pcollection_kv_4);

    PCollection<KV<String, String>> myMergedList = PCollection_KV_pairs
        .apply(Flatten.<KV<String, String>>pCollections());

    PCollection<String> PCollectionLinksString = myMergedList.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((myMergeLstout) -> myMergeLstout.toString()));

    // Concept #2: Apply a FlatMapElements transform the PCollection of text lines.
    // This transform splits the lines in PCollection<String>, where each element is
    // an
    // individual word in Shakespeare's collected texts.
    // .apply(
    // FlatMapElements.into(TypeDescriptors.strings())
    // .via((String line) -> Arrays.asList(line.split("[^\\p{L}]+"))))
    // // We use a Filter transform to avoid empty word
    // .apply(Filter.by((String word) -> !word.isEmpty()))
    // Concept #3: Apply the Count transform to our PCollection of individual words.
    // The Count
    // transform returns a new PCollection of key/value pairs, where each key
    // represents a
    // unique word in the text. The associated value is the occurrence count for
    // that word.
    // .apply(Count.perElement())
    // Apply a MapElements transform that formats our PCollection of word counts
    // into a
    // printable string, suitable for writing to an output file.
    // .apply(
    // MapElements.into(TypeDescriptors.strings())
    // .via(
    // (KV<String, Long> wordCount) ->
    // wordCount.getKey() + ": " + wordCount.getValue()))
    // Concept #4: Apply a write transform, TextIO.Write, at the end of the
    // pipeline.
    // TextIO.Write writes the contents of a PCollection (in this case, our
    // PCollection of
    // formatted strings) to a series of text files.
    //
    // By default, it will write to a set of files with names like
    // wordcounts-00001-of-00005
    PCollectionLinksString.apply(TextIO.write().to("GiridharOutput"));

    p.run().waitUntilFinish();
  }

  private static PCollection<KV<String, String>> GiridharMapper01(Pipeline p, String dataFile, String dataFolder) {
    String dataPath = dataFolder + "/" + dataFile;
    PCollection<String> pcolInputLines = p.apply(TextIO.read().from(dataPath));
    PCollection<String> pcolLines = pcolInputLines.apply(Filter.by((String line) -> !line.isEmpty()));
    PCollection<String> pcColInputEmptyLines = pcolLines.apply(Filter.by((String line) -> !line.equals(" ")));
    PCollection<String> pcolInputLinkLines = pcColInputEmptyLines
        .apply(Filter.by((String line) -> line.startsWith("[")));

    PCollection<String> pcolInputLinks = pcolInputLinkLines.apply(
        MapElements.into(TypeDescriptors.strings())
            .via((String linkline) -> linkline.substring(linkline.indexOf("(") + 1, linkline.indexOf(")"))));

    PCollection<KV<String, String>> pcollectionkvLinks = pcolInputLinks.apply(
        MapElements.into(
            TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
            .via(linkline -> KV.of(dataFile, linkline)));

    return pcollectionkvLinks;
  }

}