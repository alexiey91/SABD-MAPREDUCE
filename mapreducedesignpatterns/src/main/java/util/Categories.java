package util;


import java.util.ArrayList;

/**
 * Created by root on 22/05/17.
 */
public class Categories {
    String genres;
    Double ratingNumber;
    Double ratingAvg;
    Double ratingVar;


    public Categories(String genres) {
        super();
        this.genres = genres;
    }

    public Categories(String genres, Double ratingNumber, Double ratingAvg, Double ratingVar) {
        this.genres = genres;
        this.ratingNumber = ratingNumber;
        this.ratingAvg = ratingAvg;
        this.ratingVar = ratingVar;
    }

    public Categories() {
        this.ratingNumber = 0.0;
        this.ratingAvg = 0.0;
        this.ratingVar = 0.0;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public Double getRatingNumber() {
        return ratingNumber;
    }

    public Double getRating() {
        return ratingAvg;
    }
    public Double getRatingVar() {
        if( this.ratingNumber == 0.0)
            return 0.0;
        return this.ratingVar/(this.ratingNumber-(Double)1.0);
    }
    /* ratingSum è la media di tutti i rating di un film */

    public void addRating(Double rate) {
        Double oldAvg = this.ratingAvg;
        this.ratingNumber += 1.0;
        this.ratingAvg += (rate - this.ratingAvg)/(this.ratingNumber);
        this.ratingVar += (rate - this.ratingAvg)*(rate - oldAvg);

    }
    /*media ponderata*/
    public void addRatingMod(Double n,Double rate,Double var) {
        this.ratingNumber += n;
        this.ratingAvg += (rate - this.ratingAvg)*n/this.ratingNumber;
        this.ratingVar += (var - this.ratingVar )*n/this.ratingNumber;

    }
    public static ArrayList<Categories> addExclusive(ArrayList<Categories> source,String category,
                                              Double n,Double rate,Double var){
        ArrayList<Categories> temp = source;
        if(temp.size()==0) temp.add(new Categories(category,n,rate,var));
        else{
            for(int i =0 ; i< temp.size();i++) {
                //System.out.println("EXCLUSIVE"+category+"***"+source.get(i).getGenres());
                if (source.get(i).getGenres().equals(category)) {
                    if(n>(Double)0.0)
                        temp.get(i).addRatingMod(n, rate, var);//aggiorni i vecchi valori legati alla categoria
                    return temp;
                }
            }
            temp.add(new Categories(category,n,rate,var));//crei una nuova categoria nell'array
            //temp.add(new Categories(category));//crei una nuova categoria nell'array
        }
        return temp;
    }
}
