from sqlalchemy.orm import Session
from datetime import date, timedelta
from databasequery import QuerySet_O
import csv
import pandas as pd
from dotenv import load_dotenv
import os
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import smtplib
from email.header import Header

load_dotenv()

class SendOutput:
	
	def mailset(self, receiver_email):
		filedate = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
		# 生成唯一的 Message-ID
		self.message_id = str(uuid.uuid4())

		# 建立一個 MIMEMultipart 對象，並設置寄件人、收件人和主題
		self.message = MIMEMultipart()
		self.message["From"] = os.getenv('sender_email')
		self.message["To"] = receiver_email
		self.message["Subject"] = f"{filedate}業績訂單"
		self.message["Message-ID"] = f"<{self.message_id}@eastdawnjp.com>"  
		body = f"附件為-{filedate}業績訂單。"
		self.message.attach(MIMEText(body, "plain"))
		# 添加附件
		att = MIMEApplication(open(f'./output/{filedate}業績訂單.xlsx','rb').read(), 'utf-8')
		# att.add_header('Content-Disposition', 'attachment', filename=f'{filedate}業績訂單.xlsx')
		
		att.add_header('Content-Disposition', 'attachment', filename=Header(f'{filedate}業績訂單.xlsx', 'utf-8').encode())
		self.message.attach(att)
	
	def connectSMtp(self):
		# 設置 SMTP 服務器的地址和端口，這裡以 Gmail 的 SMTP 為例
		self.smtp_server = os.getenv("smtp_server")
		self.smtp_port = 25

		# 登錄郵件帳號
		self.smtp_username = os.getenv("smtp_username")
		self.smtp_password = os.getenv("smtp_password")

		# 建立 SMTP 連接
		self.server = smtplib.SMTP(self.smtp_server, self.smtp_port)
		self.server.starttls()  # 啟用 TLS 加密

		# 登錄郵件帳號
		self.server.login(self.smtp_username, self.smtp_password)
	
	def send_email(self, receiver_email):
		self.receiver_email = receiver_email
		self.server.sendmail(os.getenv('sender_email'), self.receiver_email, self.message.as_string())
		self.server.quit()

		print("Email sent successfully!") 
	pass


class Csv:
	def __init__(self, path):
		self._path = path

	def write(self, headers, output):
		try:
			with open(self._path, mode='x', encoding='utf8') as f:
				csvwriter = csv.writer(f)
				csvwriter.writerow(headers)
				csvwriter.writerows(output)
		except FileExistsError:
			with open(self._path, mode='a', encoding='utf8') as f:
				csvwriter = csv.writer(f)
				csvwriter.writerows(output)
		f.close()

	def to_excel(self):
		csvfile = pd.read_csv(self._path)
		csvfile.to_excel(self._path.replace('csv', 'xlsx'), index=False, header=True)


class DailyOutput:
	
	def __init__(self, start_date=None, end_date=None):
		self.start_date = start_date
		self.end_date = end_date
		
	def _hostList(self):
			hostO = {
			'host': os.getenv('hostO'),
			'user': os.getenv('user'),
			'password': os.getenv('passwordO'),
			'ssl': os.getenv('ssl_pathO'),
			'database': os.getenv('datebaseO'),
			}
			hostN = {
				'host': os.getenv('hostN'),
				'user': os.getenv('user'),
				'password': os.getenv('passwordN'),
				'ssl': os.getenv('ssl_pathN'),
				'database': os.getenv('datebaseN'),
			}
			return hostO, hostN

	def output(self):
		filedate = (date.today() - timedelta(days=1)).strftime("%Y%m%d")
		path = f'./output/{filedate}業績訂單.csv'
		csv = Csv(path)
		hostO, hostN = self._hostList()
		for database in list(hostO['database'].split(',')):
			query_instance = QuerySet_O(host=hostO['host'], user=hostO['user'], password=hostO['password'], ssl=hostO['ssl'], database=database)
			print(database)

			with Session(query_instance.engine) as session:
				result = session.execute(query_instance.query(self.start_date, self.end_date))
				csv.write(result.keys(), result.all())
		csv.to_excel()



d = DailyOutput()
d.output()

mail = SendOutput()

for reciver in list(os.getenv('receivers').split(",")):
    mail.mailset(reciver)
    mail.connectSMtp()
    mail.send_email(reciver)