import os
import sys
import time
from zipfile import ZipFile
import zipfile
from pprint import pprint
import xml.etree.ElementTree as et
from datetime import date, datetime, timedelta
import pandas as pd
import tableauserverclient as tsc

	

AUTH = dict()
logging = dict() 


class DataStruct:
	WORKBOOKS = dict()



class ServerAuth(object):

	def __init__(self):
		
		self._server = None
		self._username = None
		self._password = None
		self.is_signed_in = False
		self.auth = dict()

	@property
	def server(self):
		"""get server name"""
		return self._server
	@property
	def username(self):
		"""get username"""
		return self._username
	@property
	def password(self):
		"""get password"""
		return self._password
	@server.setter
	def server(self, srvr: str):
		self._server = str(srvr)
	@username.setter
	def username(self, un: str):
		self._username = str(un)
	@password.setter
	def password(self, pw: str):
		self._password = str(pw)

	def login(self):
		"""login to server"""
		auth_ = tsc.TableauAuth(self.username, self.password)
		print(auth_)
		srvr_name = 'http://' + str(self.server)
		print(srvr_name)
		server_ = tsc.Server(srvr_name)
		entry = {'Auth': auth_, 'Server': server_, 'Login Time': datetime.now}
		# --- update the authe dictionary
		self.auth.update(entry)
		server_.auth.sign_in(auth_)
		self.is_signed_in = True

	def logout(self):
		"""logout of server"""
		if self.is_signed_in:
			self.auth.get('Server')
			server.auth.sign_out()
			self.is_signed_in = False
			


class Workbook(ServerAuth):
	"""get workbook information"""

	def __init__(self):
		super().__init__()
		self.workbooks = None
		self.current_workbook_ = None
		self.current_wb_id_ = None
		self.downloaded_workbook_ = None
		self.file_storage_path = None
		self.from_twbx_file = False
		self.twb_file = None
		
	def get_workbook_list(self):
		"""return a dict of workbooks
		stored on the server"""
		server = self.auth.get('Server')
		all_workbooks, pagination_item = server.workbooks.get()
		self.workbooks = {wb.name: wb.id for wb in all_workbooks}

	def current_workbook(self, wb_name: str):
		"""set workbook name"""
		try:
			if len(list(self.workbooks.keys())) > 0:
				if wb_name in list(self.workbooks.keys()):
					self.current_workbook_ = str(wb_name)
					self.current_wb_id_ = self.workbooks.get(str(wb_name))
				else:
					print("Workbook name not found.")
			else:
				raise KeyError("Err: Workbook dict either not populated or workbok does not exist\n")
		except KeyError as e:
			print(str(e))

	def download_workbook(self, path_to_dl: str):
		"""download the workbook"""
		self.file_storage_path = str(path_to_dl)
		server = self.auth.get('Server')
		server.workbooks.download(str(self.current_wb_id_), filepath=path_to_dl, no_extract=False)
		wbs = [str(self.current_workbook_)+'.twb', str(self.current_workbook_)+'.twbx'] 
		for i, _ in enumerate(wbs):
			if wbs[i] in os.listdir(str(path_to_dl)):
				print("Workbook located...\n")
				self.downloaded_workbook_ = path_to_dl + '\\' + wbs[i]
				break

	def open_workbook_xml(self):
		"""check to see if tableau workbook is zipped"""
		if self.downloaded_workbook_ is not None:
			if zipfile.is_zipfile(self.downloaded_workbook_):
				self.from_twbx_file = True
				twbx_ = ZipFile(self.downloaded_workbook_)
				twbx_.extractall(path=self.file_storage_path)
				path_files = [f for _, _, f in os.walk(self.file_storage_path) if len(f) > 0]
				for file in path_files:
					for sub in file:
						current = sub.split(".")
						for j, _ in enumerate(current):
							if current[j] == 'twb':
								self.twb_file = sub
								print(self.twb_file)
			else:
				print("workbook is a .twb file, no extracting needed...\n")
				self.twb_file = self.downloaded_workbook_
	
	def update_parameters(self, param_name: str, tag_name: str, save=False):
		"""update the tableau workbook parameters"""
		parsed_twb_file = et.parse(self.twb_file)
		root = parsed_twb_file.getroot()
		current_parameter = "'[{}]'".format(param_name)
		xml_search_query = ".//*[@name={}]/{}".format(current_parameter, tag_name)
		QUERY = root.find(xml_search_query)
		query_children = QUERY.getchildren()

		# --- setup for date field extraction
		date_lambda = datetime.strptime((query_children[-1].attrib\
														   .get('value')\
														   .strip('#')\
														   .strip(' 00:00:00')), "%Y-%m-%d")
		current_date = date.today()
		day_difference = int(str(date.today() - date(date_lambda.year, 
									date_lambda.month, date_lambda.day)).split(" ")[0])
		
		# --- update parameter subtree
		date_tag = "#{}#".format(current_date)
		attribute = QUERY.makeelement('member', {'value': date_tag})
		print("adding attribute: {} ...".format(attribute))
		QUERY.append(attribute)
		if save:
			pprint(parsed_twb_file)
			#parsed_twb_file.write(self.file_storage_path+ '//' + self.twb_file)

	def build_archive(self, temp_file: str) -> None:
		"""build archive file"""
		os.chdir(self.file_storage_path)
		print("current directory: {}".format(os.getcwd()))
		for _, _, f in os.walk(str(os.getcwd())):
			for file in f:
				current = file.split(".")
				if len(current) == 2 and current[1] == 'twbx':
					os.remove(file)
					print("removing {}.".format(file))
					break
			break
		with ZipFile(str(temp_file), "w", compression=ZIP_DEFLATED) as archive:
			for root_dir, _, f in os.walk(self.file_storage_path):
				relative_dir = os.path.relpath(root_dir, self.file_storage_path)
				for file in f:
					temp_file_full_path = os.path.join(
						self.file_storage_path, relative_dir, f)
					zipname = os.path.join(relative_dir, f)
					archive.write(temp_file_full_path, arcname=zipname)

	def test_build_archive(self) -> None:
		try:
			if self.downloaded_workbook_ is not None:
				os.chdir("C:\\Users\\Administrator\\Desktop\\TableauWorkbookFiles")
				TEMP_TWB_INPUT_FILE  = ZipFile(self.downloaded_workbook_, "r")
				TEMP_TWB_OUTPUT_FILE = ZipFile('WORKBOOK.twbx', 'w')
				for item in TEMP_TWB_INPUT_FILE.infolist():
					buff = TEMP_TWB_INPUT_FILE.read(item.filename)
					if item.filename[-4:] != '.twb':
						TEMP_TWB_OUTPUT_FILE.writestr(item, buff)
				TEMP_TWB_OUTPUT_FILE.write("QueueSummary_py.twb")
				TEMP_TWB_OUTPUT_FILE.close()
				TEMP_TWB_OUTPUT_FILE.close()
			else:
				raise Exception("Err: tableau file does not exist.\n")
		except Exception as e:
			print(str(e))

		"""
        os.chdir('C:\\Users\\wmurphy\\Desktop\\workbooks')
        twb_in = zipfile.ZipFile('18.twbx', 'r')
        twb_out = zipfile.ZipFile('WORKBOOK.twbx', 'w')
        for item in twb_in.infolist():
            buffer = twb_in.read(item.filename)
            if (item.filename[-4:] != '.twb'):
                twb_out.writestr(item, buffer)
        twb_out.write("Tableau_workbook_2018-02-27.twb")
        twb_out.close()
        twb_in.close()


		"""		




				


def main():

	# --- setup
	#tabsrvr = ServerAuth()
	tab_workbook_ = Workbook()
	tab_workbook_.server = '172.31.32.54'
	tab_workbook_.username = 'Administrator'
	tab_workbook_.password = '=%vT8AFMj$'
	
	# --- login
	login_ = tab_workbook_.login()
	#print(tab_workbook_.is_signed_in)
	
	# --- get workbook info
	tab_workbook_.get_workbook_list()
	#pprint(tab_workbook_.workbooks)

	# --- set current workbook
	tab_workbook_.current_workbook(wb_name='Queue Summary Dashboards DW Version')
	tab_workbook_.download_workbook(path_to_dl='C:\\Users\\Administrator\\Desktop\\TableauWorkbookFiles')
	pprint(tab_workbook_.current_workbook_)
	pprint(tab_workbook_.current_wb_id_)
	tab_workbook_.open_workbook_xml()
	wait_time = 0
	os.chdir('C:\\Users\\Administrator\\Desktop\\TableauWorkbookFiles')
	
	while True:
		print("waiting ...\n")
		print("elapsed time: {}".format(wait_time))
		time.sleep(1)
		wait_time += 1
		if os.path.exists('Queue Summary Dashboards DW Version.twb'):
			tab_workbook_.update_parameters(param_name="Parameter 5", tag_name='members', save=True)
			break

	tab_workbook_.test_build_archive()
	



if __name__ == '__main__':
	main()
		






		
	
