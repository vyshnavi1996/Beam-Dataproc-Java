package org.edu.nwmissouri.vyshnavi;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.beam.sdk.values.KV;

public class RankedPageVyshnavi implements Serializable, Comparator<KV<Double,String>>{
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<VotingPageVyshnavi> voters = new ArrayList<VotingPageVyshnavi>();
    public RankedPageVyshnavi() {
    }
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
    public int compare(KV<Double, String> r1, KV<Double, String> r2) {
        double rank1 = r1.getKey();
        double rank2 = r2.getKey();
        if (rank1 > rank2) {
            return 1;
        } else if(rank1 < rank2) {
            return -1;
        }else{
            return 0;
        }
    }
    @Override
    public String toString(){
    return ("ThisPageName = "+ name +", ThisPageRank = "+this.rank +" ArrayListOfPages = " + this.voters);
    } 
}
