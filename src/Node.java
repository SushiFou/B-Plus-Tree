import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


public abstract class Node {

    ArrayList<Integer> keys = new ArrayList<Integer>();
    BPlusTree.InnerNode parent;
    // ReadWriteLock lock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock rrwLock   = new ReentrantReadWriteLock();
    final Lock                   readLock  = rrwLock.readLock();
    final Lock                   writeLock = rrwLock.writeLock();
    //Lock lock2= new ReentrantLock();

    abstract void delete(Integer key) throws Exception;

    abstract boolean isOverflow();

    abstract boolean isUnderflow();

    abstract Node split() throws Exception;
}