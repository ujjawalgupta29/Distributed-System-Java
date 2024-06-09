package cluster.management;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {
    private static final String REGISTRY_ZNODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZnode = null;

    private List<String> allServicesAddressed = null;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryZnode();
    }

    public void registerToCluster(String metadata) throws InterruptedException, KeeperException {
        this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_", metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to Service Registry");
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
    }

    public synchronized List<String> getAllServiceAddresses() {
        if(allServicesAddressed == null) {
            try {
                updateAddresses();
            } catch (InterruptedException e) {
            } catch (KeeperException e) {
            }
        }
        return allServicesAddressed;
    }

    public void unregisterFromCluster() {
        try {
            if(currentZnode != null && zooKeeper.exists(currentZnode, false) != null) {
                zooKeeper.delete(currentZnode, -1);
            }
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void updateAddresses() throws InterruptedException, KeeperException {
        List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, this);
        List<String> addresses = new ArrayList<>(workerZnodes.size());

        for(String workerZnode : workerZnodes) {
            String workerZnodePath = REGISTRY_ZNODE + "/" + workerZnode;
            Stat stat = zooKeeper.exists(workerZnodePath, false);
            if(stat == null) {
                continue;
            }

            byte[] addressesBytes = zooKeeper.getData(workerZnodePath, false, stat);
            String address = new String(addressesBytes);
            addresses.add(address);
        }

        this.allServicesAddressed = Collections.unmodifiableList(addresses);
        System.out.println("Cluster addresses are: " + this.allServicesAddressed);
    }

    public void createServiceRegistryZnode() {
        try {
            if(zooKeeper.exists(REGISTRY_ZNODE, false) == null) {
                zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        try {
            updateAddresses();
        } catch (InterruptedException e) {
        } catch (KeeperException e) {
        }
    }
}
