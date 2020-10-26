import java.util.ArrayList;


public abstract class Node {

    ArrayList<Integer> keys = new ArrayList<Integer>();
    BPlusTreeMulti.InnerNode parent;
    boolean lock = false ;

    abstract Data getData(Integer key);

    abstract void deleteData(Integer key);

    abstract boolean isOverflow();

    abstract Node split() throws Exception;
}