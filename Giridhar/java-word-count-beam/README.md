#word-count-beam

## Commands I used for this project : 

### checking java version 
      java --version
      
 ### checking maven version
      mvn --version

### Creeating a project.
       mvn archetype:generate `
         -D archetypeGroupId=org.apache.beam `
         -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
         -D archetypeVersion=2.36.0 `
         -D groupId=org.example `
         -D artifactId=word-count-beam `
         -D version="0.1" `
         -D package=org.apache.beam.examples `
         -D interactiveMode=false
         
         
 ### Executing a project.
          mvn compile exec:java -D exec.mainClass=org.apache.beam.giridhar.MinimalPageRankAddagalla 

## Execute using DirectRunner

```PowerShell
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner
```
