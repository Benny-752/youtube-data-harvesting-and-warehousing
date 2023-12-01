# mongodb server connection , database and collection creation
from pymongo import MongoClient
client = MongoClient("<enter your mongo client address>") 
db = client["youtube_data_lake_db"]  
collection = db["channels_data_collection"] 

# mysql server connection , tables creation
import pymysql
host = 'localhost'
port = 3307
user = 'root'
password = 'password'
conn = pymysql.connect(host=host, port=port, user=user, password=password)
cursor = conn.cursor()
cursor.execute("USE ytdb")

cursor.execute('''
CREATE TABLE IF NOT EXISTS channels (
        Channel_Name TEXT,
        Channel_ID VARCHAR(255) PRIMARY KEY,
        Channel_Description TEXT,
        Subscription_Count INTEGER,
        Channel_Views BIGINT,
        Channel_Video_Count INTEGER
    ) ''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS playlists (
        Playlist_Id VARCHAR(255) PRIMARY KEY,
        Channel_ID VARCHAR(255),
        Playlist_Name TEXT,
        FOREIGN KEY (Channel_ID) REFERENCES channels(Channel_ID) ON DELETE CASCADE
    )
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS videos ( 
    VP_Id VARCHAR(255) PRIMARY KEY,
    Video_Id VARCHAR(255),
    Video_Name TEXT,
    Video_Description TEXT,
    PublishedAt DATETIME,
    View_Count INTEGER,
    Like_Count INTEGER,
    Favorite_Count INTEGER,
    Duration TEXT,
    Thumbnail TEXT,
    Playlist_Id VARCHAR(255),
    FOREIGN KEY (Playlist_Id) REFERENCES playlists(Playlist_Id) ON DELETE CASCADE
) ''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS comments (
    CVP_Id VARCHAR(255) PRIMARY KEY,
    Comment_Id VARCHAR(255),
    Comment_Text TEXT,
    Comment_Author VARCHAR(255),
    Comment_PublishedAt DATETIME,
    Video_Id VARCHAR(255),
    FOREIGN KEY (Video_Id) REFERENCES videos(VP_Id) ON DELETE CASCADE
)
''')

conn.commit()
conn.close()