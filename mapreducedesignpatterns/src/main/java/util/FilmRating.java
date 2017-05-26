package util;

/**
 * Created by alessandro on 26/05/2017.
 */
public class FilmRating {
    String title;
    Double ratingNumberPrev;
    Double ratingNumberLast;
    Double ratingAvgLast;
    Double ratingAvgPrev;


    public FilmRating() {
        this.ratingNumberPrev = 0.0;
        this.ratingNumberLast = 0.0;
        this.ratingAvgPrev = 0.0;

        this.ratingAvgLast = 0.0;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }


    public Double getRatingNumberPrev() {
        return ratingNumberPrev;
    }

    public void setRatingNumberPrev(Double ratingNumberPrev) {
        this.ratingNumberPrev = ratingNumberPrev;
    }

    public Double getRatingNumberLast() {
        return ratingNumberLast;
    }

    public void setRatingNumberLast(Double ratingNumberLast) {
        this.ratingNumberLast = ratingNumberLast;
    }

    public Double getRatingAvgLast() {
        return ratingAvgLast;
    }

    public void setRatingAvgLast(Double ratingAvgLast) {
        this.ratingAvgLast = ratingAvgLast;
    }

    public Double getRatingAvgPrev() {
        return ratingAvgPrev;
    }

    public void setRatingAvgPrev(Double ratingAvgPrev) {
        this.ratingAvgPrev = ratingAvgPrev;
    }

    /* ratingSum è la media di tutti i rating di un film */
    public void addRatingPrev(Double rate) {

        this.ratingNumberPrev += 1.0;
        this.ratingAvgPrev += (rate - this.ratingAvgPrev)/(this.ratingNumberPrev);

    }

    public void addRatingLast(Double rate) {

        this.ratingNumberLast += 1.0;
        this.ratingAvgLast += (rate - this.ratingAvgLast)/(this.ratingNumberLast);

    }
}
