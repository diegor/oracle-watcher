package tekgenesis.oracle.watcher.notifier;

/**
 * Console Notifier
 */
public class ConsoleNotifier implements Notifier {
    @Override
    public void notify(String notification) {
        System.out.println(notification);
    }
}
