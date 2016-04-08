package tekgenesis.oracle.watcher.notifier;

/**
 * Notifier interface
 */
public interface Notifier {
    /** Send notification */
    void notify(String notification) throws NotificationException;
}
