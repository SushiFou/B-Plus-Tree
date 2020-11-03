
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class AppTest {
    //#region Function de base  
    static Callable<Void> toCallable(final Runnable runnable) {
        return new Callable<Void>() {
            @Override
            public Void call() {
                runnable.run();
                return null;
            }
        };
    }
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
    static void Write_file(String file_name, long[][] results){
        try( BufferedWriter bw = new BufferedWriter(new FileWriter(file_name))) {

            for (int i = 0; i < results.length; i++) {
                for (int j = 0; j < results[i].length; j++) {
                    bw.write(results[i][j] + ((j == results[i].length-1) ? "" : ","));
                }
                bw.newLine();
            }
            bw.flush();
        } catch (IOException e) {}
    }
    static int moyenne (long[]array){
        long somme = 0;
        for (long nombre:array){
           somme += nombre;
        }
        return (int) somme / array.length;
   
    }
    public static void print2D(long mat[][]) 
    { 
        // Loop through all rows 
        for (int i = 0; i < mat.length; i++) 
  
            // Loop through all elements of current row 
            for (int j = 0; j < mat[i].length; j++) {
                    System.out.print(mat[i][j] + "\t "); 
            }
            
        System.out.println();
    } 
    //#endregion

    //#region Runnable
    static Runnable add_Job(BPlusTreeMulti b, int val, String info) {
        Runnable task = () -> {
            try {
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
                b.search(keytosearch);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        return task;
    }

    static Runnable delete_Job(BPlusTreeMulti b, int keytodel) {
        Runnable task = () -> {
            try {
                b.delete_multi(keytodel);
            } catch (Exception e) {
                e.printStackTrace();
            }
        };
        return task;
    }
    //#endregion

    //#region Tests
    static long[][] test_BL(ArrayList<Data> list_Data,  int nb_epoch, int degree)
            throws NumberFormatException, Exception {

        int nb_col = (int) Math.log(list_Data.size());
        long[][] matrice = new long[3][nb_col];
        long start = 0;
        long end = 0;
        for (int i = 0; i < nb_col; i++) {
            //int indexfin = 100 * (int) Math.pow(10, i);
            int indexfin = (i + 1) *(int) list_Data.size()/nb_col;
            matrice[0][i] = indexfin;
            ArrayList<Data> list_Data_sub = new ArrayList<Data>(list_Data.subList(0, indexfin));
            long[] tab_main = new long[nb_epoch];
            long[] tab_multi = new long[nb_epoch];
            for (int j = 0; j < nb_epoch; j++) {
                int divideFactor = 300;

                BPlusTreeMulti b = new BPlusTreeMulti("", degree);
                start = System.currentTimeMillis();
                b.bulk_loading(list_Data_sub, 0.7f);
                end = System.currentTimeMillis();

                BPlusTreeMulti b1 = new BPlusTreeMulti("", degree);
                ExecutorService pool = Executors.newCachedThreadPool();
                ArrayList<Callable<Void>> tasks = new ArrayList<>();
                for (int k= 0; k < list_Data_sub.size() / divideFactor; k++) {
                    int fromIndex = k * divideFactor;
                    int toIndex = (k + 1) * divideFactor;
                    tasks.add( toCallable(() -> {
                        try {
                            b1.bulk_loading(new ArrayList<Data>(list_Data_sub.subList(fromIndex, toIndex)), 0.7f);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        
                    }));
                }
                tab_main[j]=end - start;
                start = System.currentTimeMillis();
                end=0;
                try {
                    pool.invokeAll(tasks);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    //TODO: handle exception
                }
                pool.shutdown();
                tab_multi[j]=end - start;
            }
            matrice[1][i]=moyenne(tab_main);
            matrice[2][i]=moyenne(tab_multi);
        }


        return matrice;
    }
    static long[][] test_Add(ArrayList<Data> list_Data,  int nb_epoch, int degree)
            throws NumberFormatException, Exception {

        int nb_col = (int) Math.log(list_Data.size());
        long[][] matrice = new long[3][nb_col];
        long start = 0;
        long end = 0;
        for (int i = 0; i < nb_col; i++) {
            //int indexfin = 100 * (int) Math.pow(10, i);
            int indexfin = (i + 1) *(int) list_Data.size()/nb_col;
            matrice[0][i] = indexfin;
            ArrayList<Data> list_Data_sub = new ArrayList<Data>(list_Data.subList(0, indexfin));
            long[] tab_main = new long[nb_epoch];
            long[] tab_multi = new long[nb_epoch];
            for (int j = 0; j < nb_epoch; j++) {
                int divideFactor = 300;

                BPlusTreeMulti b = new BPlusTreeMulti("", degree);
                start = System.currentTimeMillis();
                b.add_file(list_Data_sub);
                end = System.currentTimeMillis();

                BPlusTreeMulti b1 = new BPlusTreeMulti("", degree);
                ExecutorService pool = Executors.newCachedThreadPool();
                ArrayList<Callable<Void>> tasks = new ArrayList<>();
                for (int k= 0; k < list_Data_sub.size() / divideFactor; k++) {
                    int fromIndex = k * divideFactor;
                    int toIndex = (k + 1) * divideFactor;
                    tasks.add( toCallable(() -> {
                        try {
                            b1.add_file_Multi(new ArrayList<Data>(list_Data_sub.subList(fromIndex, toIndex)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        
                    }));
                }
                tab_main[j]=end - start;
                start = System.currentTimeMillis();
                end=0;
                try {
                    pool.invokeAll(tasks);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    //TODO: handle exception
                }
                pool.shutdown();
                tab_multi[j]=end - start;
            }
            matrice[1][i]=moyenne(tab_main);
            matrice[2][i]=moyenne(tab_multi);
        }


        return matrice;
    }
    static long[][] test_Delet(ArrayList<Data> list_Data, int nb_epoch,int degree ,int maxToDel)
            throws NumberFormatException, Exception {
        int nb_col = (int) Math.log(maxToDel);
        long[][] matrice = new long[3][nb_col];
        long start = 0;
        long end = 0;
        for (int i = 0; i < nb_col; i++) {
            //int indexstart = i *  maxToDel/nb_col;
            int indexfin = (i + 1) *(int) maxToDel/nb_col;
            matrice[0][i] = indexfin;
            ArrayList<Data> list_Data_sub = new ArrayList<Data>(list_Data.subList(0, indexfin));
            long[] tab_main = new long[nb_epoch];
            long[] tab_multi = new long[nb_epoch];
            for (int j = 0; j < nb_epoch; j++) {
                int divideFactor = 300;

                BPlusTreeMulti b = new BPlusTreeMulti("", degree);
                b.add_file(list_Data);

                start = System.currentTimeMillis();
                for(Data d :list_Data_sub ){
                    b.delete(d.idx);
                }
                end = System.currentTimeMillis();

                BPlusTreeMulti b1 = new BPlusTreeMulti("", degree);
                b1.add_file(list_Data);

                ExecutorService pool = Executors.newCachedThreadPool();
                ArrayList<Callable<Void>> tasks = new ArrayList<>();
                for (int k= 0; k < list_Data_sub.size() / divideFactor; k++) {
                    int fromIndex = k * divideFactor;
                    int toIndex = (k + 1) * divideFactor;
                    tasks.add( toCallable(() -> {
                        try {
                            b1.delete_list_Multi(new ArrayList<Data>(list_Data_sub.subList(fromIndex, toIndex)));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        
                    }));
                }
                tab_main[j]=end - start;
                start = System.currentTimeMillis();
                end=0;
                try {
                    pool.invokeAll(tasks);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    //TODO: handle exception
                }
                pool.shutdown();
                tab_multi[j]=end - start;
            }
            matrice[1][i]=moyenne(tab_main);
            matrice[2][i]=moyenne(tab_multi);
        }


        return matrice;
    }
    static long[][] test_Search(ArrayList<Data> list_Data, int nb_epoch,int degree, int maxToSearch)
            throws NumberFormatException, Exception {

        BPlusTreeMulti b = new BPlusTreeMulti("", degree);
        b.add_file(list_Data);

        int nb_col = (int) Math.log(maxToSearch);
        long[][] matrice = new long[3][nb_col];
        long start = 0;
        long end = 0;
        for (int i = 0; i < nb_col; i++) {
            int indexstart = i *  maxToSearch/nb_col;
            int indexfin = (i + 1) *(int) maxToSearch/nb_col;
            matrice[0][i] = indexfin;
            ArrayList<Data> list_Data_sub = new ArrayList<Data>(list_Data.subList(indexstart, indexstart+indexfin));
            long[] tab_main = new long[nb_epoch];
            long[] tab_multi = new long[nb_epoch];
            for (int j = 0; j < nb_epoch; j++) {

                start = System.currentTimeMillis();
                for(Data d :list_Data_sub ){
                    search_Job(b, d.idx).run();
                }
                end = System.currentTimeMillis();


                ExecutorService pool = Executors.newCachedThreadPool();
                ArrayList<Callable<Void>> tasks = new ArrayList<>();
                for(Data d :list_Data_sub ){
                    tasks.add( toCallable(() -> {search_Job(b, d.idx);}));
                }
                tab_main[j]=end - start;
                start = System.currentTimeMillis();
                end=0;
                try {
                    pool.invokeAll(tasks);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    //TODO: handle exception
                }
                pool.shutdown();
                tab_multi[j]=end - start;
            }
            matrice[1][i]=moyenne(tab_main);
            matrice[2][i]=moyenne(tab_multi);
        }


        return matrice;
    }
    static long[][] test_Mixed(ArrayList<Data> list_Data,  int nb_epoch,int degree, ArrayList<Data> Add_list,int maxOpt) throws Exception {

        //int maxOpt= Add_list.size();
        int nb_col = (int) Math.log(maxOpt);
        long[][] matrice = new long[3][nb_col];
        long start = 0;
        long end = 0;
        for (int i = 0; i < nb_col; i++) {
            int indexfin = (i + 1) *(int) maxOpt/nb_col;
            matrice[0][i] = indexfin;
            ArrayList<Data> list_Data_sub_add = new ArrayList<Data>(Add_list.subList(0, indexfin));
            ArrayList<Data> list_Data_sub_search = new ArrayList<Data>(list_Data.subList(0, indexfin));
            long[] tab_main = new long[nb_epoch];
            long[] tab_multi = new long[nb_epoch];
            for (int j = 0; j < nb_epoch; j++) {
                BPlusTreeMulti b = new BPlusTreeMulti("", degree);
                b.add_file(list_Data);

                start = System.currentTimeMillis();
                for(Data d :list_Data_sub_add ){
                    add_Job(b, d.idx, d.info);
                    
                }
                for(Data d :list_Data_sub_search ){
                    search_Job(b, d.idx).run();
                }
                // for(Data d :list_Data_sub_search ){
                //     b.delete(d.idx);
                // }
                end = System.currentTimeMillis();


                ExecutorService pool = Executors.newCachedThreadPool();
                ArrayList<Callable<Void>> tasks = new ArrayList<>();
                for(Data d :list_Data_sub_add ){
                    tasks.add( toCallable(() -> {add_Job(b, d.idx, d.info);}));
                }
                for(Data d :list_Data_sub_search ){
                    tasks.add( toCallable(() -> {search_Job(b, d.idx);}));
                }
                // for(Data d :list_Data_sub_search ){
                //     tasks.add( toCallable(() -> {delete_Job(b, d.idx);}));
                // }
                tab_main[j]=end - start;
                start = System.currentTimeMillis();
                end=0;
                try {
                    pool.invokeAll(tasks);
                    end = System.currentTimeMillis();
                } catch (Exception e) {
                    //TODO: handle exception
                }
                pool.shutdown();
                tab_multi[j]=end - start;
            }
            matrice[1][i]=moyenne(tab_main);
            matrice[2][i]=moyenne(tab_multi);
        }

        
        return matrice;
    }
    //#endregion
    public static void main(String[] args) throws Exception {
        String file = "data.csv";
        ArrayList<Data> List_Data_total = Read_file(file);
        ArrayList<Data> List_Data = new ArrayList<Data>(List_Data_total.subList(0,1000000));
        
        ////
        // long[][]res_i = test_Add(List_Data, 10, 5);
        // print2D(res_i);
        // Write_file("add.csv", res_i);
        ///////////
        // long[][]res_d = test_Delet(List_Data, 10, 5,10000);
        // print2D(res_d);
        // Write_file("Delet.csv", res_d);
        /////////
        // long[][]res_s = test_Search(List_Data, 10, 5,100000);
        // print2D(res_s);
        // Write_file("search.csv", res_s);
        // /////////////
        // ArrayList<Data> List_Data_add = new ArrayList<Data>(List_Data_total.subList(1000001,List_Data_total.size()));
        // long[][]res_m = test_Mixed(List_Data, 10, 5,List_Data_add,List_Data_add.size());
        // print2D(res_m);
        // Write_file("mixed.csv", res_m);
        /////////
        // long[][]res_bl = test_BL(List_Data, 10, 5);
        // print2D(res_bl);
        // Write_file("BL.csv", res_bl);

        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////
        //////////////////////////////////////////////////////////////////
        //a voir le delet ne marche pas trop 
        BPlusTreeMulti b = new BPlusTreeMulti("", 4);
        b.add_file(List_Data);
        //System.out.println(b.search(79057));
        //b.delete(422476);
        //System.out.println("Done!");
        int comp=0;
        for(Data d :List_Data ){
            comp++;
            System.out.print(comp+"\t");
            b.delete(d.idx);
        }
    }
    
}
