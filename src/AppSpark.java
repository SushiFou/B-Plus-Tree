

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class AppSpark {
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
   
    static long[][] test_spark(JavaRDD<BPlusTree> BplusTreeRdd,ArrayList<Data> list_Data, int nb_epoch ,int maxToSearch){

        int nb_col = (int) Math.log(maxToSearch);
        long[][] matrice = new long[2][nb_col];
        long start = 0;
        long end = 0;
        for (int i = 0; i < nb_col; i++) {
            int indexstart = i *  maxToSearch/nb_col;
            int indexfin = (i + 1) *(int) maxToSearch/nb_col;
            matrice[0][i] = indexfin;
            ArrayList<Data> list_Data_sub = new ArrayList<Data>(list_Data.subList(indexstart, indexstart+indexfin));
            long[] tab = new long[nb_epoch];
            for (int j = 0; j < nb_epoch; j++) {

                start = System.currentTimeMillis();
                for(Data d :list_Data_sub ){
                     JavaRDD< BPlusTree> b=BplusTreeRdd.filter(
                        tup -> d.idx >= tup.min &&  d.idx <= tup.max
                      );
                      b.foreach(itr->{ itr.search( d.idx);});
                }
               
                end = System.currentTimeMillis();
                tab[j]=end - start;
            }
            matrice[1][i]=moyenne(tab);

        }


        return matrice;
    }
    public static void main(String[] args)throws Exception {
        final int degree= 100;
        int nbOfPart=Runtime.getRuntime().availableProcessors();
        SparkConf conf = new SparkConf().setAppName("SparkBPlusTree").setMaster("local["+nbOfPart+"]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        String filename="generated_data.csv";
        List<Data> List_data=(List)Read_file(filename);
        JavaRDD<Data> inputRdd = sc.parallelize(List_data,nbOfPart);
        JavaRDD<Data> inputRddSorted = inputRdd.sortBy(d->d.idx, true,nbOfPart);
        
        JavaRDD<BPlusTree> BTreeRdd = inputRddSorted.mapPartitions(itr->{
            BPlusTree b = new BPlusTree("",degree);
            ArrayList<Data> list_sub = new ArrayList<Data>();
            itr.forEachRemaining(list_sub::add);
            b.add_list(list_sub);
            ArrayList<BPlusTree> res = new ArrayList<>();
            res.add(b);
            return res.iterator();
        });
        BTreeRdd.cache();
        BTreeRdd.count();
        Write_file("Spark_multi.csv", test_spark(BTreeRdd,(ArrayList)List_data,5,10000));
        System.out.printf("Done!");
        sc.close();
        
    }
    
}
