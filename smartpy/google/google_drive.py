"""
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive

class GDrive:

    def __init__(self, client_secret_json_path: str):
        self.gauth = GoogleAuth()
        self.drive = GoogleDrive(self.gauth)
        # Try to load_model saved telegram_client credentials
        self.gauth.LoadClientConfigFile(client_secret_json_path)

    def listFilesFolders(self, location_path='root') -> dict:
        root_files_and_folders = self.drive.ListFile({'q': f"'{location_path}' in parents and trashed=false"}).GetList()
        dictionary = {i['title']: i['id'] for i in root_files_and_folders}
        return dictionary

    def createFolder(self, location_id: str, folder_name: str):
        file1 = self.drive.CreateFile({'title': folder_name,
                                       "parents": [{"id": location_id}],
                                       "mimeType": "apps/vnd.google-apps.folder"})
        file1.Upload()

"""

"""
#credentials_path = ROOT_DIR+'/smartpy/configs/google_drive/otho_google_api_credentials.json'
api_secret_path = ROOT_DIR+'/smartpy/configs/google_drive/google_drive_secret_othmane.json'

gdrive = GDrive(api_secret_path)

environment_name = 'dev2'

ENTITY_NAME = 'cryptostreet'
FOLDER_TO_COPY = 'prod'


root_files_folders = gdrive.listFilesFolders()
smart_universe_id = root_files_folders['smart_universe']
entity_folder_id = gdrive.listFilesFolders(smart_universe_id)[ENTITY_NAME]
entity_files_folders = gdrive.listFilesFolders(entity_folder_id)


id_to_copy = entity_files_folders[FOLDER_TO_COPY]
copy_to_folder_id = entity_folder_id

copied_file = {'title': FOLDER_TO_COPY}


file_data = gdrive.drive.auth.service.files().copy(
    fileId=id_to_copy, body=copied_file).execute()
gdrive.drive.CreateFile({'id': file_data['id']})
"""
