package tekgenesis.oracle.watcher.notifier;

import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.*;

/**
 * Mail notifier
 */
public class MailNotifier implements Notifier{
    public static final String DATABASE_JDBC_URL = "database.jdbcUrl";
    private final Properties properties;
    private final String from;
    private final String to;
    private final String user;
    private final String password;
    private String database;

    /**
     * Constructor with mail properties
    **/
    public MailNotifier(String name, Properties properties) {
        this.properties = properties;
        from = properties.getProperty("mail.smtp.from", "oracle-watcher@tekgenesis.com");
        to = properties.getProperty("mail.to");
        user = properties.getProperty("mail.smtp.user");
        password = properties.getProperty("mail.smtp.password");
        database = name;

    }

    @Override
    public void notify(String notification) throws NotificationException {


        final Session session = Session.getDefaultInstance(properties);

        Transport transport = null;
        try {
            transport = session.getTransport();

            if(user != null && password != null)
                transport.connect(properties.getProperty("mail.smtp.host"), user, password);
            else
                transport.connect();

            final MimeMessage msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(from));
            msg.addRecipients(Message.RecipientType.TO, to);


            msg.setSubject("Locks detected on "+database, "UTF-8");

            final BodyPart messageBodyPart = new MimeBodyPart();
            messageBodyPart.setContent(notification, "text/plain");
            final Multipart multipart = new MimeMultipart();
            multipart.addBodyPart(messageBodyPart);

            msg.setContent(multipart);

            transport.sendMessage(msg, msg.getAllRecipients());
        } catch (final MessagingException e) {
            throw new NotificationException(e.getMessage());
        } finally {
            if(transport != null) try {
                transport.close();
            } catch (final MessagingException e) {
                //ignore
            }
        }
    }
}
