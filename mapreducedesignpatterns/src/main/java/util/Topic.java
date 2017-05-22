package util;

import java.util.ArrayList;
import java.util.List;

public class Topic {

    String topic;
    List<String> items;

    public Topic(){
        this.topic = null;
        this.items = new ArrayList<String>();
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getTopic() {
        return topic;
    }

    public void addItem(String item){
        this.items.add(item);
    }

}
