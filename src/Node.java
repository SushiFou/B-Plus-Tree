import java.util.ArrayList;


public abstract class Node {

    ArrayList<Integer> keys = new ArrayList<Integer>();
    BPlusTree.InnerNode parent;

    abstract Data getData(Integer key);

    abstract void deleteData(Integer key);

    abstract boolean isOverflow();

    abstract Node split() throws Exception;
}