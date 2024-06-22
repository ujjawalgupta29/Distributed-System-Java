import networking.WebClient;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class Aggregator {
    private WebClient client;

    public Aggregator() {
        client = new WebClient();
    }

    public List<String> sendTasksToWorkers(List<String> workerAddresses, List<String> tasks) {
        CompletableFuture<String>[] futures = new CompletableFuture[workerAddresses.size()];

        for(int i=0; i< workerAddresses.size(); i++) {
            String workerAddress = workerAddresses.get(i);
            String task = tasks.get(i);

            byte[] payload = task.getBytes();
            futures[i] = client.sendTask(workerAddress, payload);
        }

        List<String> results = new ArrayList<>();
        for(int i=0; i<tasks.size(); i++) {
            results.add(futures[i].join());
        }

        return results;
    }
}
