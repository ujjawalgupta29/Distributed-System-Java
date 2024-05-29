import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class WatchersDemo implements Watcher{
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String TARGET_ZNODE = "/target_znode";
    private ZooKeeper zooKeeper;
    private static final int SESSION_TIMEOUT = 3000;

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        WatchersDemo watchersDemo = new WatchersDemo();
        watchersDemo.connectToZookeeper();
        watchersDemo.run();
        watchersDemo.close();
        System.out.println("Disconnected from Zookeeper, exiting app");
    }

    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void watchTargetZnode() throws InterruptedException, KeeperException {
        Stat stat = zooKeeper.exists(TARGET_ZNODE, this);
        if(stat == null) {
            return;
        }
        byte[] data = zooKeeper.getData(TARGET_ZNODE, this, stat);
        List<String> childrens = zooKeeper.getChildren(TARGET_ZNODE, this);

        System.out.println("Data : " + new String(data) + " children : " + childrens);
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None :
                if(watchedEvent.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to Zookeeper");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Recieved Disconnected event");
                        zooKeeper.notifyAll();
                    }
                }
                break;

            case NodeDeleted:
                System.out.println(TARGET_ZNODE + " was deleted");
                break;
            case NodeCreated:
                System.out.println(TARGET_ZNODE + " was created");
                break;
            case NodeDataChanged:
                System.out.println(TARGET_ZNODE + " data changed");
                break;
            case NodeChildrenChanged:
                System.out.println(TARGET_ZNODE + " children changed");
                break;
        }

        try {
            watchTargetZnode();
        } catch (InterruptedException e) {

        } catch (KeeperException e) {

        }
    }
}
