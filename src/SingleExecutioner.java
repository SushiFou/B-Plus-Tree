import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SingleExecutioner extends AbstractExecutioner {

    SingleExecutioner(BPlusTreeMulti t) {
        super(t);
    }

    @Override
    void initialize_executor() {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        this.executor = executor;
    }
    
}
