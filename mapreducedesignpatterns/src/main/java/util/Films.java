package util;


/**
 * Created by root on 22/05/17.
 */
public class Films {
    String title;
    Double ratingNumber;
    Double ratingAvg;

    public Films() {
        this.ratingNumber = 0.0;
        this.ratingAvg = 0.0;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public Double getRatingNumber() {
        return ratingNumber;
    }

    public Double getRating() {
        return ratingAvg;
    }
    /* ratingSum è la media di tutti i rating di un film */
    public void addRating(Double rate) {

        this.ratingNumber += 1.0;
        this.ratingAvg += (rate - this.ratingAvg)/(this.ratingNumber);

    }
}
