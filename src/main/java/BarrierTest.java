import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;

public class BarrierTest {

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder().connectString("192.168.64.10:30718")
                .retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        client.start();
        //String result = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ZKPaths.makePath("huq2", "cisco"), "yes".getBytes());
        //System.out.println(result);
        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(client, "/distributedBarrier", 3);
        for (int i = 0;i < 3; i++) {
            new Thread(() -> {
                try {
                    System.out.println(Thread.currentThread().getName() + "is trying to enter the barrier");
                    barrier.enter();
                    System.out.println(Thread.currentThread().getName() + "has already entered the barrier");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }).start();
        }
        //barrier.enter();
    }
}
