import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AppMulti {
    static ArrayList<Data> Read_file(String file_name) throws NumberFormatException, IOException {
        ArrayList<Data> res = new ArrayList<Data>();
        try (BufferedReader br = new BufferedReader(new FileReader(file_name))) {
            String line;
            Boolean firstline = true;
            while ((line = br.readLine()) != null) {
                String[] value = line.split(";");
                if (firstline) {
                    firstline = false;
                } else {
                    res.add( new Data(value[1],Integer.parseInt(value[0])));
                }
            }
        }
        return res;
    }

    static Runnable add_Job(BPlusTreeMulti b, int val, String info) {
        Runnable task = () -> {
            try {
                //System.out.println("!!!!!!Adding Job");
                b.rootLock.lock();
                b.add(val, new Data(info, val));
                b.rootLock.unlock();
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        return task;
    }

    static Runnable search_Job(BPlusTreeMulti b, int keytosearch) {
        Runnable task = () -> {
            try {
                Data res = b.search(keytosearch);
                //System.out.println(res);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        return task;
    }


    static Callable<Void> toCallable(final Runnable runnable) {
        return new Callable<Void>() {
            @Override
            public Void call() {
                runnable.run();
                return null;
            }
        };
    }
    public static void main(String[] args) throws Exception {
        String file = "data.csv";
        int divideFactor = 300;
        ArrayList<Data> List_Data = Read_file(file);

        BPlusTreeMulti test = new BPlusTreeMulti("", 4);
        long start = System.currentTimeMillis();
        test.add_file(List_Data);
        long end = System.currentTimeMillis();
        System.out.printf("time For Insert on Main: %d ms\n\n", end - start);
        ///
        BPlusTreeMulti test1 = new BPlusTreeMulti("", 4);
        ExecutorService pool = Executors.newCachedThreadPool();
        ArrayList<Callable<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < List_Data.size() / divideFactor; i++) {
            int fromIndex = i * divideFactor;
            int toIndex = (i + 1) * divideFactor;
            tasks.add( toCallable(() -> {
                try {
                    test1.add_file_Multi(new ArrayList<Data>(List_Data.subList(fromIndex, toIndex)));
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
            }));
        }
        
        start = System.currentTimeMillis();
        end=0;
        try {
            pool.invokeAll(tasks);
            end = System.currentTimeMillis();
        } catch (Exception e) {
            //TODO: handle exception
        }
        pool.shutdown();
        System.out.printf("time For Insert on Multi : %d ms\n\n", end - start);
        /////////////////////test2///////////////////////////////
        //long 
        // start = System.currentTimeMillis();

        // Thread thread_S_1_1 = new Thread(search_Job(test,300));
        // thread_S_1_1.start();
        // Thread thread_S_1_2 = new Thread(search_Job(test,400));
        // thread_S_1_2.start();
        // Thread thread_S_1_3 = new Thread(search_Job(test,30));
        // thread_S_1_3.start();
        // Thread thread_S_1_4 = new Thread(search_Job(test,500));
        // thread_S_1_4.start();
        // Thread thread_S_1_5 = new Thread(search_Job(test,600));
        // thread_S_1_5.start();

        // Thread thread = new Thread(add_Job(test,2000,"SY"));
        // thread.start();

        // Thread thread_S_2_1 = new Thread(search_Job(test,310));
        // thread_S_2_1.start();
        // Thread thread_S_2_2 = new Thread(search_Job(test,410));
        // thread_S_2_2.start();
        // Thread thread_S_2_3 = new Thread(search_Job(test,31));
        // thread_S_2_3.start();
        // Thread thread_S_2_4 = new Thread(search_Job(test,510));
        // thread_S_2_4.start();
        // Thread thread_S_2_5 = new Thread(search_Job(test,610));
        // thread_S_2_5.start();

        
        // thread_S_1_1.join();
        // thread_S_1_2.join();
        // thread_S_1_3.join();
        // thread_S_1_4.join();
        // thread_S_1_5.join();
        // thread.join();
        // thread_S_2_1.join();
        // thread_S_2_2.join();
        // thread_S_2_3.join();
        // thread_S_2_4.join();
        // thread_S_2_5.join();
        // //long 
        // end = System.currentTimeMillis();
        // System.out.printf("time 5/1/5 Search Multi: %d ms\n\n", end - start);
        // // ///
        
        // BPlusTreeMulti test1 = new BPlusTreeMulti("",4);
        // test1.add_file("data.csv");

        // start = System.currentTimeMillis();
        // search_Job(test,300).run();
        // search_Job(test,400).run();
        // search_Job(test,30).run();
        // search_Job(test,500).run();
        // search_Job(test,600).run();

        // add_Job(test,2000,"SY").run();

        // search_Job(test,310).run();
        // search_Job(test,410).run();
        // search_Job(test,31).run();
        // search_Job(test,510).run();
        // search_Job(test,610).run();

        // end = System.currentTimeMillis();

        // System.out.println();
        // System.out.printf("time 5/1/5 Main: %d ms\n\n", end - start);
        /////////////////////test3///////////////////////////////
        
        ExecutorService pool2 = Executors.newCachedThreadPool();
        ArrayList<Callable<Void>> tasks_2 = new ArrayList<>();
        int epoch = 100;
        for (int i = 0; i < epoch; i++) {
            //System.out.print(i+">");
            tasks_2.add(toCallable(search_Job(test,300)));
            tasks_2.add(toCallable(search_Job(test,400)));
            tasks_2.add(toCallable(search_Job(test,30)));
            tasks_2.add(toCallable(search_Job(test,500)));
            tasks_2.add(toCallable(search_Job(test,600)));
            tasks_2.add(toCallable(add_Job(test,2000+i,"SY")));
            tasks_2.add(toCallable(search_Job(test,310)));
            tasks_2.add(toCallable(search_Job(test,410)));
            tasks_2.add(toCallable(search_Job(test,31)));
            tasks_2.add(toCallable(search_Job(test,510)));
            tasks_2.add(toCallable(search_Job(test,610)));
        
        }
        

        start = System.currentTimeMillis();
        end=0;
        try {
            pool2.invokeAll(tasks_2);
            end = System.currentTimeMillis();
        } catch (Exception e) {
            //TODO: handle exception
        }
        pool2.shutdown();
        System.out.println();
        System.out.printf("time 5/1/5 Search Multi: %d ms\n\n", end - start);
        // ///
        
        // BPlusTreeMulti test1 = new BPlusTreeMulti("",4);
        // test1.add_file("data.csv");

        start = System.currentTimeMillis();
        // test.search(300);
        // test.search(400);
        // test.search(30);
        // test.search(500);
        // test.search(600);

        // test.add(2002, new Data("SY",2002));

        // test.search(310);
        // test.search(410);
        // test.search(31);
        // test.search(510);
        // test.search(610);
        for (int i = 0; i < epoch; i++) {
            //System.out.print(i+">");
            search_Job(test,300).run();
            search_Job(test,400).run();
            search_Job(test,30).run();
            search_Job(test,500).run();
            search_Job(test,600).run();

            add_Job(test,2010+i,"SY").run();

            search_Job(test,310).run();
            search_Job(test,410).run();
            search_Job(test,31).run();
            search_Job(test,510).run();
            search_Job(test,610).run();
        }

        end = System.currentTimeMillis();

        System.out.println();
        System.out.printf("time 5/1/5 Search Main: %d ms\n\n", end - start);
    }
}
