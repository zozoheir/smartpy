import smtplib
from email.message import EmailMessage
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

class Email:
    def __init__(self, smtp_server, smtp_port, smtp_user, smtp_password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password
        self.msg = MIMEMultipart()
        self.msg['From'] = smtp_user

    def set_recipients(self, to_emails):
        self.msg['To'] = ', '.join(to_emails)

    def set_subject(self, subject):
        self.msg['Subject'] = subject

    def set_body(self, body, is_html=False):
        if is_html:
            self.msg.attach(MIMEText(body, 'html'))
        else:
            self.msg.attach(MIMEText(body, 'plain'))

    def add_attachment(self, file_path):
        with open(file_path, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={file_path}')
        self.msg.attach(part)

    def send(self):
        with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
            server.starttls()
            server.login(self.smtp_user, self.smtp_password)
            server.send_message(self.msg)


"""
# Send email
    email = Email(smtp_server='smtp-relay.brevo.com',
                  smtp_port=587,
                  smtp_user='jo.webber.jo@gmail.com',
                  smtp_password='sKD5dH2GN1Uk67R9')
    email_list = []
    email_list = ['zozoheir@umich.edu']
    email.set_recipients(email_list)
    email.set_subject(f'JET: Daily options report')
    email.set_body('<h4>Your daily options report is attached</4>', is_html=True)
    email.add_attachment(pdf_file_path)
    email.send()
"""