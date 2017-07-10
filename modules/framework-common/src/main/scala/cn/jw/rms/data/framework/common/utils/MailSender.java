package cn.jw.rms.data.framework.common.utils;

/**
 * Created by deanzhang on 16/1/15.
 */

import java.util.List;
import java.util.Properties;
import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class MailSender {

    private Session session;

    public MailSender(String host, int port, boolean auth, String username, String passwd) {
        Properties props = new Properties();
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.port", port);
        props.put("mail.smtp.auth", auth);

        final String uname = username;
        final String pass = passwd;
        session = Session.getInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(uname, pass);
                    }
                });
    }


    public String send(String subject, String body, String from, List<String> to) {
        try {
            Message msg = new MimeMessage(session);
            msg.setFrom(new InternetAddress(from));

            Address[] addrs = new Address[to.size()];
            for (int i = 0; i < to.size(); i++) {
                addrs[i] = new InternetAddress(to.get(i));
            }
            msg.addRecipients(Message.RecipientType.TO, addrs);
            msg.setSubject(subject);
            msg.setText(body);
            Transport.send(msg);

            return "Send mail [" + subject + "] succeed";

        } catch (MessagingException e) {
            e.printStackTrace();
            return "Send mail [" + subject + "] failed, " + e.toString();
        }
    }


}
