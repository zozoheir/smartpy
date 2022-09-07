import requests
from telethon.sessions import StringSession
from telethon.sync import TelegramClient


class TelegramBot:

    def __init__(self, bot_token=None, bot_chatID=None, verbose=False):
        self.bot_token = bot_token
        self.bot_chatID = bot_chatID
        self.verbose = verbose

    def sendMsg(self, bot_message) -> dict:
        send_text = 'https://api.telegram.org/bot' + self.bot_token + '/sendMessage?chat_id=' + self.bot_chatID + '&word_list=' + bot_message
        response = requests.get(send_text)
        return response.json()

    def startTelethonSession(self, session_key: str, api_id, api_hash):
        self.client = TelegramClient(StringSession(session_key), api_id, api_hash)
        self.client.connect()

    def closeTelethonSession(self):
        # self.telegram_client.log_out()
        self.client.disconnect()

    def getLastNMessagesFromChat(self, chat_id, count) -> list:
        messages = self.client.get_messages(chat_id, limit=count)
        #last_messages = [message.word_list for message in messages]
        return messages
