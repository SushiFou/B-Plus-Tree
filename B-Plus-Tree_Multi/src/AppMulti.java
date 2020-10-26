import java.util.ArrayList;

public class AppMulti {
    static Runnable add_Job(BPlusTreeMulti b, int val, String info) {
        Runnable task = () -> {
            try {
                //if any other thread is add at the same time 
                if (b.root == null)while(b.firstLeafNode.lock == true){
                    System.out.println("firstLeafNode is lock");
                    Thread.sleep(200);
                }
                else while(b.root.lock == true){
                    System.out.println("root is lock");
                    Thread.sleep(200);
                }
                ////
                if (b.root == null)b.firstLeafNode.lock = true;
                else b.root.lock = true;
                b.add(val, new Data(info),false);
                if (b.root == null)b.firstLeafNode.lock = false;
                else b.root.lock = false;
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
                System.out.println(res);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        return task;
    }
    public static void main(String[] args) throws Exception  {

        BPlusTreeMulti test = new BPlusTreeMulti(4);
        //test.add_file("data.csv", true);
        ///////////////////Test1////////////////////////////////
        // ca marche pas trop a revoir 
        ArrayList<Thread> listThread = test.add_file_Multi("data.csv", true);
        for ( Thread t:listThread){
            t.join();
        }
        test.print_Tree();
        ///////////////////Test2////////////////////////////////
        //new Thread(add_Job(test,0,"NAN")).start();
        //new Thread (search_Job(test,1)).start();

        ///////////////////Test3////////////////////////////////
        // long start = System.currentTimeMillis();

        // Thread thread = new Thread(add_Job(test,0,"NAN"));
        // thread.start();

        // Thread thread2 = new Thread(search_Job(test,0));
        // thread2.start();
        // Thread thread3 = new Thread(add_Job(test,4,"NAN1"));
        // thread3.start();
        // Thread thread4 = new Thread(search_Job(test,4));
        // thread4.start();

        // thread.join();
        // thread2.join();
        // thread3.join();
        // thread4.join();
        // long end = System.currentTimeMillis();
        // System.out.printf("time: %d ms\n\n", end - start);
        // ///
        
        // BPlusTreeMulti test1 = new BPlusTreeMulti(4);
        // test1.add_file("data.csv", false);

        // start = System.currentTimeMillis();
        // add_Job(test1,0,"NAN").run();
        // search_Job(test1,0).run();
        // add_Job(test1,4,"NAN1").run();
        // search_Job(test1,4).run();

        // end = System.currentTimeMillis();

        // System.out.println();
        // System.out.printf("time: %d ms\n\n", end - start);


    }
}
