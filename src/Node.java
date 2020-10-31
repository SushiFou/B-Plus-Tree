import java.util.ArrayList;


public abstract class Node {

    ArrayList<Integer> keys = new ArrayList<Integer>();
    BPlusTree.InnerNode parent;

    abstract void delete(Integer key) throws Exception;

    abstract boolean isOverflow();

    abstract boolean isUnderflow();

    abstract Node split() throws Exception;
}