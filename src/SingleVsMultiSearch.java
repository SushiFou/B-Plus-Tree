import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SingleVsMultiSearch {

    static int mean(long[] array) {
        long somme = 0;
        for (long nombre : array) {
            somme += nombre;
        }
        return (int) somme / array.length;
    }

    static void write_file(String file_name, Float[][] results) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file_name))) {
            for (int i = 0; i < results.length; i++) {
                for (int j = 0; j < results[i].length; j++) {
                    bw.write(results[i][j] + ((j == results[i].length - 1) ? "" : ","));
                }
                bw.newLine();
            }
            bw.flush();
        } catch (IOException e) {
        }
    }

    public static <T> List<List<T>> sepList(final List<T> ls, final int iParts) {
        final List<List<T>> lsParts = new ArrayList<List<T>>();
        final int iChunkSize = ls.size() / iParts;
        int iLeftOver = ls.size() % iParts;
        int iTake = iChunkSize;

        for (int i = 0, iT = ls.size(); i < iT; i += iTake) {
            if (iLeftOver > 0) {
                iLeftOver--;

                iTake = iChunkSize + 1;
            } else {
                iTake = iChunkSize;
            }
            lsParts.add(new ArrayList<T>(ls.subList(i, Math.min(iT, i + iTake))));
        }
        return lsParts;
    }

    // taken from https://www.amazon.com/dp/0201657880
    public static List<Integer> randomListCreator(int start, int end, int count) {
        Random rng = new Random();

        Integer[] randomList = new Integer[count];
        int cur = 0;
        int remaining = end - start;
        for (int i = start; i < end && count > 0; i++) {
            double probability = rng.nextDouble();
            if (probability < ((double) count) / (double) remaining) {
                count--;
                randomList[cur++] = i;
            }
            remaining--;
        }
        return (Arrays.asList(randomList));
    }

    public static void main(String[] args) throws Exception {

        // time param
        long startTime = System.currentTimeMillis();
        long endTime = System.currentTimeMillis();
        NumberFormat formatter = new DecimalFormat("#0.00000");
        // print param
        boolean printBol = false;

        // tree
        BPlusTreeMulti treeMulti = new BPlusTreeMulti("generated_data.csv", 100);
        startTime = System.currentTimeMillis();
        treeMulti.add_file("generated_data.csv");
        endTime = System.currentTimeMillis();
        System.out.println(
                "Execution time for Tree Creation " + formatter.format((endTime - startTime) / 1000d) + " seconds");

        // ---- test for searching -----

        // indexes we will search
        int datasetSize = 10000000;
        // int sampleSize = datasetSize / 5;

        // create list of sampleSize random elements between 1 and datasetSize
        List<Integer> indexToSearch = randomListCreator(1, datasetSize+1, datasetSize);
        List<List<Integer>> subListsToSearch = sepList(indexToSearch, 12); // 12 points

        // List<Integer> threads = Arrays.asList(1, 2, 4, 6, 8, 10, 12);
        List<Float> single_threads_result = new ArrayList<Float>();
        List<Float> multi_threads_result = new ArrayList<Float>();
        Float[][] results_Matrix = new Float[2][12];
        // results_Matrix[0] = new Float[] { 1f, 2f, 4f, 6f, 8f, 10f, 12f };

        for (int listToSearch_idx = 0; listToSearch_idx < subListsToSearch.size(); listToSearch_idx++) {

            
            List<Integer> listToSearch = new ArrayList<Integer>();
            for (int i = 0; i <= listToSearch_idx; i++) {
                listToSearch = Stream.concat(listToSearch.stream(), subListsToSearch.get(i).stream())
                        .collect(Collectors.toList());
            }
            System.out.println("\nCurrent index "+listToSearch_idx+" out of "+subListsToSearch.size());
            System.out.println("size of the current list "+listToSearch.size());
            
            List<List<Integer>> subLists = sepList(listToSearch, 12);

            int nb_epochs = 100;
            long[] multiThreadTimes = new long[nb_epochs];
            long[] singleThreadTimes = new long[nb_epochs];
            System.out.print("\nEpoch = ");
            for (int epoch = 0; epoch < nb_epochs; epoch++) {

                if ((epoch+1) % 10 == 0) {
                    System.out.print(epoch+1 + ",");
                }
                // MULTI thread
                // executor initialization
                MultiExecutioner mExecutioner = new MultiExecutioner(treeMulti);
                // mExecutioner.nbProcs = Runtime.getRuntime().availableProcessors();
                mExecutioner.nbProcs = 12;
                mExecutioner.initialize_executor();

                // tasks creation and affiliation
                // we create as many tasks as threads
                List<Callable<ArrayList<Data>>> tasks = new ArrayList<>();
                for (List<Integer> subList : subLists) {
                    Callable<ArrayList<Data>> task = mExecutioner.search_task(subList);
                    tasks.add(task);
                }

                // tasks execution
                startTime = System.currentTimeMillis();
                List<Future<ArrayList<Data>>> futures = mExecutioner.execute_tasks(tasks);
                endTime = System.currentTimeMillis();
                long time = endTime - startTime;
                // System.out.println("Execution time for Multithreading search "
                // + formatter.format((time) / 1000d) + " seconds");
                multiThreadTimes[epoch] = time;

                // print resutls
                if (printBol) {
                    for (Future<ArrayList<Data>> future : futures) {
                        try {
                            System.out.println(future.get());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }

                // stop all the threads
                mExecutioner.stop_executor();

                // SINGLE thread

                // executor initialization
                SingleExecutioner sExecutioner = new SingleExecutioner(treeMulti);
                sExecutioner.initialize_executor();

                // tasks creation and affiliation
                List<Callable<ArrayList<Data>>> tasksSingle = new ArrayList<>();
                Callable<ArrayList<Data>> task = sExecutioner.search_task(listToSearch);
                tasksSingle.add(task);

                // tasks execution
                startTime = System.currentTimeMillis();
                List<Future<ArrayList<Data>>> futuresSingle = sExecutioner.execute_tasks(tasksSingle);
                endTime = System.currentTimeMillis();
                time = endTime - startTime;
                // System.out.println("Execution time for Singlethreading search"
                // + formatter.format((time) / 1000d) + " seconds");
                singleThreadTimes[epoch] = time;

                // print resutls
                if (printBol) {
                    for (Future<ArrayList<Data>> future : futuresSingle) {
                        try {
                            System.out.println(future.get());
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }

                // stop all the threads
                sExecutioner.stop_executor();

            }
            single_threads_result.add(((mean(singleThreadTimes)) / 1000f));
            multi_threads_result.add(((mean(multiThreadTimes)) / 1000f));
        }
        Float[] array = new Float[single_threads_result.size()];
        single_threads_result.toArray(array); // fill the array
        results_Matrix[0] = array;
        array = new Float[multi_threads_result.size()];
        multi_threads_result.toArray(array); // fill the array
        results_Matrix[1] = array;
        write_file("multi_vs_single_search.csv", results_Matrix);

    }

}
