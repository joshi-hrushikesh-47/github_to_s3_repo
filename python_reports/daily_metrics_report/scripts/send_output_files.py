import mimetypes
import ntpath
import smtplib
from email import encoders
from email.mime.audio import MIMEAudio
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# from constants import report_date, credentials

sender_address = credentials['sender_email_credentials']['sender_address']
default_subject = "Daily Metrics Report"
smtp_server_address = "smtp.gmail.com"
smtp_port_number = 587
password = credentials['sender_email_credentials']['password']


default_message = message = """
<html>
<body>
<p>Hi Team,<br></p>
<p>Please find the daily metrics report for """ + report_date.strftime("%Y-%m-%d") + """ attached with this mail.</p>
<p>--<br>
Regards,<br>
Kirit Bhatt<br>
+91-6265331612</p>
</body>
</html>
"""
#Function for sending email
def send_by_smtp(
    to=None,
    cc=None,
    bcc=None,
    subject=None,
    attachments=None,
    attachment_type="plain",
    isTls=True,
    message=default_message
):
    """
        Snippet to send an email to multiple people along with multiple attachments.
        :param to: list of emails
        :param cc: list of emails
        :param bcc: list of emails
        :param subject: Email Subject
        :param attachments: list of file paths
        :param attachment_type: 'plain' or 'html'
        :return: None
    """
    email_from = sender_address
    email_to = list()
    files_to_send = attachments
    msg = MIMEMultipart()
    msg["From"] = email_from
    if to:
        to = list(set(to))
        email_to += to
        msg["To"] = ", ".join(to)
    if cc:
        cc = list(set(cc))
        email_to += cc
        msg["Cc"] = ", ".join(cc)
    if bcc:
        bcc = list(set(bcc))
        email_to += bcc
        msg["Bcc"] = ", ".join(bcc)
    if subject:
        msg["Subject"] = subject
        msg.preamble = subject
    else:
        msg["Subject"] = default_subject
        msg.preamble = default_subject

    body = message
    msg.attach(MIMEText(body, attachment_type))

    if files_to_send:
        for file_to_send in files_to_send:
            attachment = MIMEBase("application", "octet-stream")
            attachment.set_payload(open(file_to_send, "rb").read())
            encoders.encode_base64(attachment)
            attachment.add_header(
                "Content-Disposition",
                "attachment",
                filename=ntpath.basename(file_to_send),
            )
            msg.attach(attachment)

    try:
        smtp_obj = smtplib.SMTP(
            host=smtp_server_address,
            port=smtp_port_number)
        if isTls:
            smtp_obj.starttls()
        smtp_obj.login(sender_address, password)
        smtp_obj.sendmail(
            from_addr=email_from,
            to_addrs=list(set([email_from] + email_to)),
            msg=msg.as_string(),
        )
        print("Successfully sent email to {}".format(str(email_to)))
        smtp_obj.quit()
        return True
    except smtplib.SMTPException as e:
        print(e)
        print("Error: unable to send email")
        return False
