package org.edu.nwmissouri.vyshnavi;
import java.io.Serializable;

public class VotingPage implements Serializable{
    String name = "unknown.md";
    Double rank = 1.0;
    Integer votes = 0;

    /**
     * 
     * @param nameIn - name of contributing page
     * @param votesIn - Count of votes made by contributing page
     */
    public VotingPage(String nameIn, Integer votesIn) {
        this.name = nameIn;
        this.votes = votesIn;
    }
    /**
     * 
     * @param nameIn - name of contributing page
     * @param rankIn - rank of contributor page
     * @param votesIn - Count of votes made by contributing page
     */
    public VotingPage(String nameIn,Double rankIn, Integer votesIn) {
        this.name = nameIn;
        this.rank = rankIn;
        this.votes = votesIn;
    }
   
    public String getName(){
        return this.name;
    }
    public Double getRank(){
       return  this.rank;
    }
    public  Integer getVotes(){
        return this.votes;
    }
    @Override
public String toString(){
    return ("ContributorPageName = "+ name +", ContributorPageRank = "+this.rank +" CountOfVotes = " + this.votes);
}
}  
