import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public abstract class AbstractExecutioner {
    public BPlusTreeMulti Tree;
    public ExecutorService executor;

    AbstractExecutioner(BPlusTreeMulti t) {
        this.Tree = t;
    }

    abstract void initialize_executor();

    private Callable<Void> toCallable(final Runnable runnable) {
        return new Callable<Void>() {
            @Override
            public Void call() {
                runnable.run();
                return null;
            }
        };
    }

    public void stop_executor() {
        try {
            // System.out.println("attempt to shutdown executor");
            this.executor.shutdown();
            this.executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            System.err.println("tasks interrupted");
        } finally {
            if (!this.executor.isTerminated()) {
                System.err.println("cancel non-finished tasks");
            }
            this.executor.shutdownNow();
            // System.out.println("shutdown finished");
        }
    }

    public List<Future<ArrayList<Data>>> execute_tasks(List<Callable<ArrayList<Data>>> tasks) {
        List<Future<ArrayList<Data>>> futures = null;
        try {
            futures = this.executor.invokeAll(tasks);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return futures;
    }

    public void execute_Runnable_tasks(List<Runnable> tasks) {
        List<Callable<Void>> callables = new ArrayList<>();
        for (Runnable r : tasks) {
            callables.add(toCallable(r));
        }
        try {
            this.executor.invokeAll(callables);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public Callable<ArrayList<Data>> search_task(List<Integer> indexes){
        
        ArrayList<Data> results = new ArrayList<Data>();
        Callable<ArrayList<Data>> task = () -> {
            try {
                for(int key:indexes){
                    results.add(this.Tree.search(key));
                }
                return results;
            }
            catch (Exception e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
        return task;
    }

    public Runnable insert_task(List<Integer> indexes){
        
        Runnable task = () -> {
            try {
                for(int key:indexes){
                    this.Tree.rootLock.lock();
                    this.Tree.add(key, new Data("something cool"));
                    this.Tree.rootLock.unlock();
                }
            }
            catch (Exception e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
        return task;
    }

    public Runnable delete_task(List<Integer> indexes){
        
        Runnable task = () -> {
            try {
                for(int key:indexes){
                    this.Tree.rootLock.lock();
                    this.Tree.delete(key);
                    this.Tree.rootLock.unlock();
                }
            }
            catch (Exception e) {
                throw new IllegalStateException("task interrupted", e);
            }
        };
        return task;
    }

    public Callable<ArrayList<Data>> bulk_task(List<Integer> indexToSearch){
        return null;
        // see BPlusTree.java
    }
}
