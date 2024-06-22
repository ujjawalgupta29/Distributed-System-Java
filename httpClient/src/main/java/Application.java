import java.util.Arrays;
import java.util.List;

public class Application {
    private static final String WORKER_1 = "http://localhost:8081/task";
    private static final String WORKER_2 = "http://localhost:8082/task";
    public static void main(String[] args) {
        Aggregator aggregator = new Aggregator();
        String task1 = "10,200";
        String task2 = "12344,3491276982374,10000000000";
        List<String> result = aggregator.sendTasksToWorkers(Arrays.asList(WORKER_1, WORKER_2),
                Arrays.asList(task1, task2));

        for(String res : result) {
            System.out.println(res);
        }
    }
}
