// ...............................................................................................................................
//
// (C) Copyright  2011/2016 TekGenesis.  All Rights Reserved
// THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF TekGenesis.
// The copyright notice above does not evidence any actual or intended
// publication of such source code.
//
// ...............................................................................................................................
package tekgenesis.oracle.watcher;

import tekgenesis.oracle.watcher.notifier.ConsoleNotifier;
import tekgenesis.oracle.watcher.notifier.MailNotifier;
import tekgenesis.oracle.watcher.notifier.NotificationException;
import tekgenesis.oracle.watcher.notifier.Notifier;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

/** Main app class */
public class App {

    public static final String CONSOLE = "console";
    private int interval = 2;
    private boolean running = true;
    private String url = null;
    private String name = null;
    private String systemUser = null;
    private String password = null;
    private String user = "";
    private int waitingTime = 5;
    private boolean locksFound = false;
    private final List<Notifier> notifierList = new ArrayList<>();
    private StringBuffer notification = new StringBuffer();

    App(File propertiesFile) {
        try {
            final Properties properties = new Properties();
            properties.load(new FileInputStream(propertiesFile));
            initDb(properties);
            initNotifiers(properties);
        } catch (final IOException e) {
            System.err.println("Error reading properties file: "+e.getMessage());
        }


    }

    private void initNotifiers(Properties properties) {
        final String notifiers = properties.getProperty("notifiers","");
        final String[] split = notifiers.split(",");
        if(notifiers.isEmpty()) addNotifier(CONSOLE, properties);
        for (final String s : split) {
            addNotifier(s, properties);
        }

    }

    private void addNotifier(String s, Properties properties) {
        switch (s) {
            case "mail":
                notifierList.add(new MailNotifier(name, properties));
                break;
            case CONSOLE:
                notifierList.add(new ConsoleNotifier());
                break;
        }
    }

    private void initDb(Properties properties) {

        url = properties.getProperty(MailNotifier.DATABASE_JDBC_URL);
        systemUser = properties.getProperty("database.systemUser");
        password = properties.getProperty("database.systemPassword");
        user = properties.getProperty("database.user");
        waitingTime = Integer.parseInt(properties.getProperty("waitingTime","5"));
        interval = Integer.parseInt(properties.getProperty("interval", "2"));
        name = properties.getProperty("database.name",url);

        try {
            Class.forName("oracle.jdbc.OracleDriver");
        } catch (final ClassNotFoundException e) {
            System.err.println("Oracle driver not found. Please download it and place it under lib directory");
            System.exit(1);
        }
    }

    /**
    * Main method. Receives properties file name as argument
    */
    public static void main(String[] args) {
        if(args.length != 1) {
            printUsage();
            System.exit(1);
        }
        final App app = new App(new File(args[0]));
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    app.stop();
                }
                catch (final Exception e) {
                    e.printStackTrace(System.err);
                }
            }
        });
        app.start();
    }

    private static void printUsage() {
        System.err.println("usage: watch <properties file>");
    }

    private void start() {

        while (running) {
            final int sleepTime = interval * 60 * 1000;
            try {
                checkLocks();
                Thread.sleep(sleepTime);
                //ignore
            } catch (final SQLException e) {
                System.err.println("Error checking locks: "+e.getMessage());
                try {
                    Thread.sleep(sleepTime);
                } catch (final InterruptedException e1) {
                    //ignore
                }
            } catch (final InterruptedException e) {
                //ignore
            }
        }
    }

    private void checkLocks() throws SQLException {

        final Connection connection = getConnection();
        final HashMap<Integer, Session> sessions = new HashMap<>();
        if(connection != null) {
            try (ResultSet resultSet = connection.prepareStatement(getSql()).executeQuery()){
                while (resultSet.next()) {
                    final Session session = Session.create(resultSet);
                    sessions.put(session.sid, session);
                }
            }
            finally {
                connection.close();
            }
            analyzeSessions(sessions);
        }
    }

    private void analyzeSessions(HashMap<Integer, Session> sessions) {
        final Stream<Session> blockedSessions = sessions.values().stream().filter(s -> s.blockedBy != null && s.runningFor >= waitingTime).sorted();
        blockedSessions.forEach(s -> notify(lock(s, sessions.get(s.blockedBy))));
        if(notification.length() == 0 && locksFound) {
            notify("Locks have been cleared");
            locksFound = false;
        }
        else if (notification.length() > 0) locksFound = true;
        flush();
    }

    private String lock(Session s, Session blocker) {
        return "--------------------------------------------------------------------------------------------------------\n" +
                "Session " + s.key() + " is being locked by session " + blocker.key() + " by " + s.runningFor + " minutes.\n" +
                "\nInfo for session " + sessionInfo(s) +
                "\n" + "Locker: " + sessionInfo(blocker)+
                "\n--------------------------------------------------------------------------------------------------------\n";
    }

    private String sessionInfo(Session s) {
        return s.key() + ": " + s.machine+" "+s.status + " " + s.module + ":\n\t" + (s.sqlText != null ? s.sqlText : "NO SQL");
    }

    private void flush() {
        if(notification.length() > 0) {
            for (final Notifier notifier : notifierList) {
                try {
                    notifier.notify(notification.toString());
                } catch (final NotificationException e) {
                    System.err.println(e.getMessage());
                }
            }
            notification = new StringBuffer();
        }
    }

    private void notify(String s) {
        notification.append(s).append("\n");
    }

    private String getSql() {
        return "select SES.SID, SES.SERIAL#, STATUS, round(SECONDS_IN_WAIT/60) Minutes, BLOCKING_SESSION, SES.MODULE, SES.MACHINE, sql_text, EVENT " +
                "from GV$SESSION SES left join GV$SQLAREA SQL on (SES.sql_hash_value = sql.hash_value and ses.sql_address = sql.address) " +
                (!"".equals(user) ? ("where USERNAME='" + user + "'") : "") + " order by 1 desc";
    }

    private Connection getConnection() {
        try {
            return DriverManager.getConnection(url, systemUser,password);
        } catch (final SQLException e) {
            notify("Cannot connect to dabase: "+e.getMessage());
            flush();
        }
        return null;
    }

    private void stop() {
        running = false;
    }

    private static class Session implements Comparable<Session>{
        int sid;
        int serial;
        String module = "";
        String status ="";
        String machine="";
        int runningFor;
        String sqlText = "";
        Integer blockedBy = null;
        private String event = "";

        public Session(int sid) {
            this.sid = sid;
        }

        static Session create(ResultSet rs) throws SQLException {
            final Session session = new Session(rs.getInt("SID"));
            session.serial = rs.getInt("SERIAL#");
            session.status = rs.getString("STATUS");
            session.runningFor = rs.getInt("Minutes");
            final int blocking_session = rs.getInt("BLOCKING_SESSION");
            session.blockedBy = blocking_session > 0 ? blocking_session : null;
            session.module = rs.getString("MODULE");
            session.sqlText = rs.getString("sql_text");
            session.machine = rs.getString("MACHINE");
            session.event = rs.getString("EVENT");
            return session;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Session && key().equals(((Session)obj).key());
        }

        @Override
        public String toString() {
            return "("+sid+","+serial+","+machine+") - Waiting for: "+runningFor+" minutes. "+status+" "+(blockedBy != null ? blockedBy : "") + module + " "+(sqlText != null ? sqlText : "");
        }

        @Override
        public int compareTo(Session o) {
            return runningFor > o.runningFor ? -1 : (runningFor > o.runningFor ?  1 :0);
        }

        public String key() {
            return "("+sid+","+serial+")";
        }

        @Override
        public int hashCode() {
            int result = sid;
            result = 31 * result + serial;
            return result;
        }
    }
}
