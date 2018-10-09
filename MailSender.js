const nodemailer = require('nodemailer');
const util = require('util');

class MailSender {
    constructor(host, port, user, pass) {
        this.transporter = nodemailer.createTransport({
            host: host,
            port: port,
            auth: {
                user: user,
                pass: pass
            },
            tls: {
                rejectUnauthorized: false
            }
        });
    }

    sendMail(from, to, subject, text) {
        var mailOptions = {
            from: from,
            to: to,
            subject: subject,
            text: text
        };
        this.transporter.sendMail(mailOptions, (error, info) => {
            if (error) {
                console.log(util.inspect(error));
            } else {
                console.log('Email sent: ' + info.response);
            }
        });
    }
}

module.exports = MailSender;