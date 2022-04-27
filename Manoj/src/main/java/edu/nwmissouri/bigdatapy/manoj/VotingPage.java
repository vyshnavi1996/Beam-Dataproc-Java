package edu.nwmissouri.bigdatapy.manoj;


import java.io.Serializable;

public class VotingPage implements Serializable {

    String voter;
    int votes;
    double pageRank;
    public VotingPage(String voter,Integer votes, double pageRank){
        this.voter = voter;
        this.votes = votes;      
        this.pageRank = pageRank;  
    }

    public VotingPage(String voter,Integer votes){
        this.voter = voter;
        this.votes = votes;      
        this.pageRank = 1.0;  
    }
    
    public String getVoter() {
        return voter;
    }
    
    public int getVotes() {
        return votes;
    }
   
    @Override
    public String toString() {
        return "voterName : "+ voter +", Page rank : "+this.pageRank +" ContributorVotes : " + votes;
    }

    public double getPageRank() {
        return this.pageRank;
    }
  


}