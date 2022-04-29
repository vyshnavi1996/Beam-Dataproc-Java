package org.apache.beam.examples;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPage implements Serializable{
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<VotingPage> voters = new ArrayList<VotingPage>();
    /**
     * 
     * @param nameIn this page name
     * @param votersIn arraylist of pages pointing to this page
     */
    public RankedPage(String nameIn, ArrayList<VotingPage> votersIn) {
        this.name = nameIn;
        this.voters = votersIn;
    }
    /**
     * 
     * @param nameIn this page name
     * @param rankIn this page's rank
     * @param votersIn array list of pages pointing to this page
     */
    public RankedPage(String nameIn,Double rankIn, ArrayList<VotingPage> votersIn) {
        this.name = nameIn;
        this.rank= rankIn;
        this.voters = votersIn;
    }
    public String getName(){
        return this.name;
    }
    public void setName(String nameIn){
        this.name = nameIn;
    }
    public double getRank(){
        return rank;

    }
    public  ArrayList<VotingPage> getVoters(){
        return this.voters;
    }
    public  void setVoters(ArrayList<VotingPage> voters){
        this.voters = voters;
    }
@Override
public String toString(){
    return ("ThisPageName = "+ name +", ThisPageRank = "+this.rank +" ArrayListOfPages = " + this.voters);

} 
}
