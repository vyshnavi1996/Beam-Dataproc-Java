package org.apache.beam.giridhar;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import org.apache.beam.sdk.values.KV;

public class RankedPageAddagalla implements Serializable, Comparator<KV<Double,String>>{
    String name = "unknown.md";
    Double rank = 1.000;
    ArrayList<VotingPageAddagalla> voters = new ArrayList<VotingPageAddagalla>();
    public RankedPageAddagalla() {
    }
    /**
     * 
     * @param nameIn this page name
     * @param votersIn arraylist of pages pointing to this page
     */
    public RankedPageAddagalla(String nameIn, ArrayList<VotingPageAddagalla> votersIn) {
        this.name = nameIn;
        this.voters = votersIn;
    }
    /**
     * 
     * @param nameIn this page name
     * @param rankIn this page's rank
     * @param votersIn array list of pages pointing to this page
     */
    public RankedPageAddagalla(String nameIn,Double rankIn, ArrayList<VotingPageAddagalla> votersIn) {
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
    public  ArrayList<VotingPageAddagalla> getVoters(){
        return this.voters;
    }
    public  void setVoters(ArrayList<VotingPageAddagalla> voters){
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
