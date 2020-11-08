import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MultiExecutioner extends AbstractExecutioner{

    int nbProcs;

    MultiExecutioner(BPlusTreeMulti t) {
        super(t);
    }

    @Override
    void initialize_executor() {
        ExecutorService executor = Executors.newFixedThreadPool(this.nbProcs);
        // System.out.println("total threads : "+nbProcs);
        this.executor = executor;
    } 
    

}
