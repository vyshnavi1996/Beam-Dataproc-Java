package org.edu.nwmissouri.vyshnavi;

import java.io.Serializable;
import java.util.ArrayList;

public class RankedPageVyshnavi implements Serializable{
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<VotingPageVyshnavi> voters = new ArrayList<VotingPageVyshnavi>();
    /**
     * 
     * @param nameIn this page name
     * @param votersIn arraylist of pages pointing to this page
     */
    public RankedPageVyshnavi(String nameIn, ArrayList<VotingPageVyshnavi> votersIn) {
        this.name = nameIn;
        this.voters = votersIn;
    }
    /**
     * 
     * @param nameIn this page name
     * @param rankIn this page's rank
     * @param votersIn array list of pages pointing to this page
     */
    public RankedPageVyshnavi(String nameIn,Double rankIn, ArrayList<VotingPageVyshnavi> votersIn) {
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
    public  ArrayList<VotingPageVyshnavi> getVoters(){
        return this.voters;
    }
    public  void setVoters(ArrayList<VotingPageVyshnavi> voters){
        this.voters = voters;
    }
@Override
public String toString(){
    return ("ThisPageName = "+ name +", ThisPageRank = "+this.rank +" ArrayListOfPages = " + this.voters);

} 
}
