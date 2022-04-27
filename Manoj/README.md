# word-count-beam

## Commands to run word count sample file

PS> mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false

 ---------------

 PS> cd .\word-count-beam

 --------------

 PS> dir

 ----------------

PS> dir .\src\main\java\org\apache\beam\examples

-------------------

PS> mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner

 ----------------------

ls counts*

 ---------------

more counts*

