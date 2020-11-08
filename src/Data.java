//  Type of Data we process 
import java.io.Serializable;
public class Data   implements Serializable{
    String info;
    int idx;
    
    Data(String info){
        this.info = info;
        this.idx = 0;
    }
    //used in spark 
   Data(String info, int idx){
        this.info = info;
        this.idx = idx;
    }

    public String toString(){
        return this.info;
    }
}
