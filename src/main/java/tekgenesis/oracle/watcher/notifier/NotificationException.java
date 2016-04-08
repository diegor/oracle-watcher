package tekgenesis.oracle.watcher.notifier;

/**
 * Notification Exception
 */
public class NotificationException extends Throwable {
    private static final long serialVersionUID = -7238955066853063582L;

    /**
     *  Contructor with exception message
     */
    public NotificationException(String message) {
        super(message);
    }
}
