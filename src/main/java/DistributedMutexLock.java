import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributedMutexLock {

    private CuratorFramework client;
    private String path;
    private ConcurrentHashMap<Thread, LockData> lockDataMap = new ConcurrentHashMap<>();
    private Watcher watcher = (event -> notifyAll());
    private static class LockData {
        private String lockPath;
        private AtomicInteger lockCount = new AtomicInteger(1);
        private Thread currentHeldingThread;

        public LockData(String lockPath, Thread currentHeldingThread) {
            this.lockPath = lockPath;
            this.currentHeldingThread = currentHeldingThread;
        }
    }

    public DistributedMutexLock(CuratorFramework client, String path) {
        this.client = client;
        this.path = path;
    }

    public void acquire() throws Exception {
        LockData lockData = lockDataMap.get(Thread.currentThread());
        if (lockData != null) {
            lockData.lockCount.incrementAndGet();
            return;
        }
        for(;;) {
            boolean nodeExists = false;
            try {
                client.create().forPath(path);
            } catch (KeeperException.NodeExistsException e) {
                nodeExists = true;
            }
            if (!nodeExists) {
                lockDataMap.put(Thread.currentThread(), new LockData(path, Thread.currentThread()));
                return;
            }
            synchronized(this) {
                Stat stat = client.checkExists().usingWatcher(watcher).forPath(path);
                if (stat != null) {
                    wait();
                }
            }
        }
    }

    public void release() throws Exception {
        LockData lockData = lockDataMap.get(Thread.currentThread());
        if (lockData == null) {
            throw new RuntimeException("the lock has not been held by current thread");
        }
        if (lockData.lockCount.getAndDecrement() > 1) {
            return;
        }
        lockDataMap.remove(Thread.currentThread());
        

    }
}
