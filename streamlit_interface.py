from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from pymongo import MongoClient
from pymongo import MongoClient
import json
from datetime import datetime
import sys
import pandas as pd
import streamlit as st
st.set_page_config(layout="wide")

#------------------------------------google API KEY------------------------------
API_KEY='<enter your api key>' 
youtube = build('youtube', 'v3', developerKey=API_KEY)
import mysql.connector
#------------------------------------mongodb---------------------------------------
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017") 
db = client["youtube_data_lake_db"]  
collection = db["channels_data_collection"]  

#--------------------------------------mysql---------------------------------------
import pymysql
host = 'localhost'
port = 3307
user = 'root'
password = 'pri123'
conn = mysql.connector.connect(host=host, port=port, user=user, password=password)
cursor = conn.cursor(buffered=True)
cursor.execute("USE ytdb")

#------------------------------------channel id------------------------------------
def get_channel_id(channel_name):
        response = youtube.search().list(
            part='id',
            q=channel_name,
            type='channel'
        ).execute()
        channel = response['items'][0]
        channel_id = channel['id']['channelId']
        return channel_id

#-------------------------------------check channel id-----------------------------
def is_valid_channel(channel_id):
        response = youtube.channels().list(
            part='snippet',
            id=channel_id
        ).execute()
        return response['items']

#-------------------------------------channel details------------------------------
def get_channel_details(channel_id):
    response = youtube.channels().list(
        part='snippet,statistics,contentDetails',
        id=channel_id
    ).execute()
    channel_details = response['items'][0]
    return channel_details

#-------------------------------------playlists------------------------------------
def get_channel_playlists(channel_id):
        response = youtube.playlists().list(
            part='snippet',
            channelId=channel_id,
            maxResults=100  # Adjust the maximum number of playlists 
        ).execute()
        playlists = response['items']
        channel_playlists={}
        for playlist in playlists:
            playlist_id = playlist['id']
            playlist_title = playlist['snippet']['title']
            channel_playlists[playlist_title]=playlist_id
        return channel_playlists

#--------------------------------------videos--------------------------------------
def get_playlist_videos(playlist_id):
    response = youtube.playlistItems().list(
        part='snippet',
        playlistId=playlist_id,
        maxResults=50
    ).execute()

    videos = response['items']
    playlist_videos = {}
    if not videos:
        return playlist_videos

    for video in videos:
        video_id = video['snippet']['resourceId']['videoId']
        video_title = video['snippet']['title']

        video_info = youtube.videos().list(
            part='snippet,statistics,contentDetails',
            id=video_id
        ).execute()
        if 'items' not in video_info or len(video_info['items']) == 0:
            continue # Skip videos without any information available
        video_data = video_info['items'][0]
        video_details = {
            'Video_Id': video_id,
            'Video_Name': video_title,
            'Video_Description': 'video_data[snippet][description]',
            'Tags': video_data['snippet'].get('tags', []),
            'PublishedAt': video_data['snippet']['publishedAt'],
            'View_Count': video_data['statistics'].get('viewCount', 0),
            'Like_Count': video_data['statistics'].get('likeCount', 0),
            'Favorite_Count': video_data['statistics'].get('favoriteCount', 0),
            'Duration': video_data['contentDetails']['duration'],
            'Thumbnail': video_data['snippet']['thumbnails']['default']['url'],
            'Comments': {}
        }
        try:
            comment_response = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=100 # Adjust the maximum number of comments 
            ).execute()
            comments = comment_response['items']
            for comment in comments:
                comment_id = comment['id']
                comment_text = 'comment[snippet][topLevelComment][snippet][textDisplay]'
                comment_author = comment['snippet']['topLevelComment']['snippet']['authorDisplayName']
                comment_published_at = comment['snippet']['topLevelComment']['snippet']['publishedAt']

                comment_details = {
                    'Comment_Id': comment_id,
                    'Comment_Text': comment_text,
                    'Comment_Author': comment_author,
                    'Comment_PublishedAt': comment_published_at
                }
                video_details['Comments'][comment_id] = comment_details
            playlist_videos[video_id] = video_details
        except HttpError as e:
            if e.resp.status == 403 and b'commentsDisabled' in e.content:
                video_details['Comments'] = {}
    return playlist_videos

#--------------------------------------convert mongodb to mysql---------------------
def migrate_data_to_sql(channel_name_input):

    # check if channel entered exists in mongodb
    unique_channels_mongodb = collection.distinct('Channel_ID')
    channels_mongodb=[]
    for value in unique_channels_mongodb:
        channels_mongodb.append(value)
    if channel_name_input not in channels_mongodb:
        return 'Please enter a channel that is present in mongodb'

    # check if channel already present in mysql
    select_query = f'''SELECT Channel_ID FROM channels WHERE Channel_ID = "{channel_name_input}"'''
    cursor.execute(select_query)
    existing_channel = cursor.rowcount
    if existing_channel>0:
        return "Channel already exists in the database."

    channels_table={}
    playlist_instant={}
    playlists_table=[]
    video={}
    videos_table=[]
    comment={}
    comments_table=[]

    # converting mongodb document to mysql tables
    query = {"Channel_ID": channel_name_input}
    complete_channel_info = collection.find(query)
    for subpart_channel_info in complete_channel_info:
        for channel_field in subpart_channel_info: 
            if channel_field=='Channel_Name':
                Channel_Name = subpart_channel_info[channel_field]
                channels_table['Channel_Name']=Channel_Name
            if channel_field=='Channel_ID':
                Channel_ID=subpart_channel_info[channel_field]
                channels_table['Channel_ID']=Channel_ID
                playlist_instant['Channel_ID']=Channel_ID
            if channel_field=='Subscription_Count':
                Subscription_Count=subpart_channel_info[channel_field]
                channels_table['Subscription_Count']=Subscription_Count
            if channel_field=='Channel_Views':
                Channel_Views=subpart_channel_info[channel_field]
                channels_table['Channel_Views']=Channel_Views
            if channel_field=='Channel_Description':
                Channel_Description='subpart_channel_info[Channel_Description]'
                channels_table['Channel_Description']=Channel_Description
            if channel_field=='Playlist_Name':
                Playlist_Name=subpart_channel_info[channel_field]
                playlist_instant['Playlist_Name']=Playlist_Name
            if channel_field=='Channel_Video_Count':
                Channel_Video_Count=subpart_channel_info[channel_field]
                channels_table['Channel_Video_Count']=Channel_Video_Count
            if channel_field=='Playlist_Id':
                Playlist_Id=subpart_channel_info[channel_field]
                playlist_instant['Playlist_Id']=Playlist_Id
            if channel_field=='Videos_Details':
                for video_instant in  subpart_channel_info[channel_field]:
                    for video_entry in subpart_channel_info[channel_field][video_instant]:  
                        if video_entry=='Video_Id':
                            Video_Id=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['Video_Id']=Video_Id
                        if video_entry=='Video_Name':
                            Video_Name=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['Video_Name']=Video_Name
                        if video_entry=='Video_Description':
                            Video_Description='subpart_channel_info[channel_field][video_instant][video_entry]'
                            video['Video_Description']=Video_Description
                        if video_entry=='PublishedAt':
                            PublishedAt=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['PublishedAt']=PublishedAt
                        if video_entry=='View_Count':
                            View_Count=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['View_Count']=View_Count
                        if video_entry=='Like_Count':
                            Like_Count=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['Like_Count']=Like_Count
                        if video_entry=='Favorite_Count':
                            Favorite_Count=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['Favorite_Count']=Favorite_Count
                        if video_entry=='Comment_Count':
                            Comment_Count=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['Comment_Count']=Comment_Count
                        if video_entry=='Duration':
                            Duration=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['Duration']=Duration
                        if video_entry=='Thumbnail':
                            Thumbnail=subpart_channel_info[channel_field][video_instant][video_entry]
                            video['Thumbnail']=Thumbnail
                        if video_entry =='Comments':
                            for comment_instant in subpart_channel_info[channel_field][video_instant][video_entry]:
                                for comment_entry in subpart_channel_info[channel_field][video_instant][video_entry][comment_instant]:
                                    if comment_entry=='Comment_Id':
                                        Comment_Id=subpart_channel_info[channel_field][video_instant][video_entry][comment_instant][comment_entry]
                                        comment['Comment_Id']=Comment_Id
                                    if comment_entry=='Comment_Text':
                                        Comment_Text='subpart_channel_info[channel_field][video_instant][video_entry][comment_instant][comment_entry]'
                                        comment['Comment_Text']=Comment_Text
                                    if comment_entry=='Comment_Author':
                                        Comment_Author=subpart_channel_info[channel_field][video_instant][video_entry][comment_instant][comment_entry]
                                        comment['Comment_Author']=Comment_Author
                                    if comment_entry=='Comment_PublishedAt':
                                        Comment_PublishedAt=subpart_channel_info[channel_field][video_instant][video_entry][comment_instant][comment_entry]
                                        comment['Comment_PublishedAt']=Comment_PublishedAt
                                vpid=''.join([Playlist_Id,Video_Id])
                                comment['Video_Id']=vpid
                                cvpid=''.join([Playlist_Id,Video_Id,Comment_Id])
                                comment['CVP_Id']=cvpid
                                comments_table.append(comment)
                                comment={}
                    video['Playlist_Id']=Playlist_Id
                    vpid=''.join([video['Playlist_Id'],video['Video_Id']])
                    video['VP_Id']=vpid
                    videos_table.append(video)
                    video={}
        playlists_table.append(playlist_instant)
        playlist_instant={}

    # insert into channels table
    query = '''
        INSERT INTO channels (Channel_Name, Channel_ID, Channel_Description, Subscription_Count, Channel_Views, Channel_Video_Count)
        VALUES (%(Channel_Name)s, %(Channel_ID)s, %(Channel_Description)s, %(Subscription_Count)s, %(Channel_Views)s, %(Channel_Video_Count)s)
    '''
    values = {
        'Channel_Name': channels_table['Channel_Name'],
        'Channel_ID': channels_table['Channel_ID'],
        'Channel_Description': channels_table['Channel_Description'],
        'Subscription_Count': int(channels_table['Subscription_Count']),
        'Channel_Views': int(channels_table['Channel_Views']),
        'Channel_Video_Count': int(channels_table['Channel_Video_Count'])
    }
    cursor.execute(query, values)

    p_count = 0
    v_count = 0  
    o_count = 0  

    # insert into playlists table
    for playlist in playlists_table:
        query = '''
            INSERT INTO playlists (Channel_ID, Playlist_Name, Playlist_Id)
            VALUES (%(Channel_ID)s, %(Playlist_Name)s, %(Playlist_Id)s)
        '''
        values = {
            'Channel_ID': playlist['Channel_ID'],
            'Playlist_Name': playlist['Playlist_Name'],
            'Playlist_Id': playlist['Playlist_Id']
        }
        cursor.execute(query, values)
        p_count +=cursor.rowcount
    
    # insert into videos table 
    for video in videos_table:
        query = '''
            INSERT INTO videos (VP_Id, Video_Id, Video_Name, Video_Description, PublishedAt, View_Count, Like_Count, Favorite_Count, Duration, Thumbnail, Playlist_Id)
            VALUES (%(VP_Id)s, %(Video_Id)s, %(Video_Name)s, %(Video_Description)s, %(PublishedAt)s, %(View_Count)s, %(Like_Count)s, %(Favorite_Count)s, %(Duration)s, %(Thumbnail)s, %(Playlist_Id)s)
        '''
        values = {
            'VP_Id': video['VP_Id'],
            'Video_Id': video['Video_Id'],
            'Video_Name': video['Video_Name'],
            'Video_Description': video['Video_Description'],
            'PublishedAt': datetime.strptime(video['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ'),
            'View_Count': int(video['View_Count']),
            'Like_Count': int(video['Like_Count']),
            'Favorite_Count': int(video['Favorite_Count']),
            'Duration': video['Duration'],
            'Thumbnail': video['Thumbnail'],
            'Playlist_Id': video['Playlist_Id']
        }
        cursor.execute(query, values)
        v_count += cursor.rowcount 

    # insert into comments table
    if comments_table:
        for comment in comments_table:

            comment_published_at = datetime.strptime(comment['Comment_PublishedAt'], '%Y-%m-%dT%H:%M:%SZ')
            comment_published_at_formatted = comment_published_at.strftime('%Y-%m-%d %H:%M:%S')
            query = '''
                INSERT INTO comments (CVP_Id, Comment_Id, Comment_Text, Comment_Author, Comment_PublishedAt, Video_Id)
                VALUES (%(CVP_Id)s, %(Comment_Id)s, %(Comment_Text)s, %(Comment_Author)s, %(Comment_PublishedAt)s, %(Video_Id)s)
            '''
            values = {
                'CVP_Id': comment['CVP_Id'],
                'Comment_Id': comment['Comment_Id'],
                'Comment_Text': comment['Comment_Text'],
                'Comment_Author': comment['Comment_Author'],
                'Comment_PublishedAt': comment_published_at_formatted,
                'Video_Id': comment['Video_Id']
            }
            cursor.execute(query, values)
            o_count += cursor.rowcount 
    conn.commit()
    return f'Channel Name:{channels_table["Channel_Name"]} <br>Number of Playlist Inserted:{p_count} <br>Number of Videos Inserted:{v_count}<br>Number of Comments Inserted:{o_count}'

#---------------------------------------main--------------------------------------------------------
col1,col2 = st.columns(2)
with col1:
    st.image("https://images.indianexpress.com/2023/06/YouTube-Pixabay.jpg",width=500)
with col2:
    st.write("---")
    st.subheader("Youtube Data Harvesting and Warehousing:      \n"
                 "Project using MongodB and SQL \n"
                 "1. User Channel Details entry \n"
                 "2. Database Operation (mongodB/SQL) \n"
                 "3. SQl Queries")
st.write("---")

max_height = 200  
style = f"max-height: {max_height}px; overflow-y: auto;"

#--------------------------------------get youtube data, show mongodb, show mysql--------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown("<h3 style='text-align: left; color:  rgb(138, 138, 138);'>Store YouTube Channel Data</h3>", unsafe_allow_html=True)
    select_box = st.radio("Select YouTube Channel Input Type", ("Channel Name", "Channel ID") )
    if select_box == "Channel Name":
        channel_identity = st.text_input("Enter YouTube Channel Name:")
    else:
        channel_identity = st.text_input("Enter YouTube Channel ID:")
    submitted = st.button("Submit")
channel_found=1
if submitted:
    if select_box=='Channel Name':
            try:
                channel_id = get_channel_id(channel_identity)
            except Exception as e:
                st.write(e)
                with col1:
                    st.write("Channel name not found or Invalid")
                channel_found=0
    elif select_box=='Channel ID':
            try:
                is_valid_channel(channel_identity)
                channel_id=channel_identity
            except Exception as e:
                with col1:
                    st.write("Channel ID not found or Invalid")
                channel_found=0
    if channel_found==1:
        channel_details=get_channel_details(channel_id) 
        channel_playlists=get_channel_playlists(channel_id)
        if len(channel_playlists)==0:
            with col1:
                st.write('This Youtube channel has 0 playlists or no access to available playlists')
        data_string=[]
        data_string.append(f"Youtube Channel Name: {channel_details['snippet']['title']}")
        for index, (key, value) in enumerate(channel_playlists.items()):
            playlist_videos={}
            playlist_videos = get_playlist_videos(value)
            if len(playlist_videos)==0:
                data=f'{index+1} {key} contains 0 videos (or) no access to playlist. Playlist not inserted'
                data_string.append(f"<span style='color: red;'>{data}</span>")
            else:
                document = {}
                document['Channel_Name'] = channel_details['snippet']['title']
                document['Channel_ID'] = channel_id
                document['Channel_Description'] = channel_details['snippet']['description']
                document['Subscription_Count'] = channel_details['statistics']['subscriberCount']
                document['Channel_Views'] = channel_details['statistics'].get('viewCount', 0)#channel_details['statistics']['viewCount']
                document['Playlist_Name'] = channel_details['snippet']['description']
                document['Channel_Video_Count'] = channel_details['statistics']['videoCount']
                document['Playlist_Name'] = key
                document['Playlist_Id'] = value        
                document['Videos_Details'] = playlist_videos
                #print(json.dumps(document, indent=4))
                # Insert channel details to mongodb
                existing = collection.find_one({ 'Playlist_Id': value })
                if existing:
                    data=f'{index+1} {key} playlist already exists. Skipping insertion.'
                    data_string.append(f"<span style='color: blue;'>{data}</span>")
                else:
                    insert_result = collection.insert_one(document)
                    inserted_id = insert_result.inserted_id
                    data = f"Playlist {index+1}: {key} contains {len(playlist_videos)} videos. Playlist inserted"
                    data_string.append(f"<span style='color: green;'>{data}</span>")
        data='<br>'.join(data_string)
        with col1:
            st.markdown(f"<div style='{style}'>{data}</div>", unsafe_allow_html=True)
else:
    st.markdown(f"<div style='{style}'>{'<br><br><br><br><br>'}</div>", unsafe_allow_html=True)

# display mongodb datalake
max_height = 400
style = f"max-height: {max_height}px; overflow-y: auto;"
unique_channels_mongodb = collection.distinct('Channel_Name')
with col2:
    st.markdown("<h3 style='text-align: left;color:rgb(138, 138, 138);'>YouTube Channels in MongoDB</h3>", unsafe_allow_html=True)
    unique_channels_mongodb = collection.distinct('Channel_Name', {'Channel_ID': {'$exists': True}})
    m_c_list=[]
    for channel_name in unique_channels_mongodb:
        channel_id = collection.find_one({'Channel_Name': channel_name})['Channel_ID']
        m_c_list.append(channel_name+' ( ID: '+channel_id+' )')
    channels_mongodb=[]
    for index, value in enumerate(m_c_list, start=1):
        channel = f"{index}. {value}"
        channels_mongodb.append(f"<span style='color:rgb(138, 138, 138);font-weight: bold;'>{channel}</span>")
    data='<br>'.join(channels_mongodb)
    st.markdown(f"<div style='{style}'>{data}</div>", unsafe_allow_html=True)

# display mysql database   
with col3:
    st.markdown("<h3 style='text-align: left;color:rgb(138, 138, 138);'>YouTube Channels in Mysql</h3>", unsafe_allow_html=True)
    cursor.execute("SELECT Channel_Name, Channel_ID FROM channels")
    channels_sql= cursor.fetchall()
    s_c_l=[]
    for s_c in channels_sql:
        s_c_l.append(s_c[0]+' ( ID: '+s_c[1]+' )')
    s_c_l=sorted(s_c_l)
    channels_mongodb=[]
    for index, value in enumerate(s_c_l, start=1):
        channel = f"{index}. {value}"
        channels_mongodb.append(f"<span style='color:rgb(138, 138, 138));font-weight: bold; '>{channel}</span>")
    data='<br>'.join(channels_mongodb)
    st.markdown(f"<div style='{style}'>{data}</div>", unsafe_allow_html=True)
    print(data)

#------------------------------------------------Database operations--------------------------------------
st.markdown("---")  
st.markdown("<h2 style='text-align: center; color:rgb(138, 138, 138);'>Database Operation</h2>", unsafe_allow_html=True)
st.markdown("---") 
colum1,colum2,colum3=st.columns(3)
max_height = 250
style = f"max-height: {max_height}px; overflow-y: auto;"

# migrate data from mongodb to mysql
with colum1:
    st.markdown("<h3 style='text-align: left; color:rgb(138, 138, 138);'>Migrate from MongoDB to MySQL</h3>", unsafe_allow_html=True)
    input_channel=st.text_input("Enter YouTube Channel ID")
    submit = st.button("Migrate")
    if submit:
         status=migrate_data_to_sql(input_channel)
         st.markdown(f"<div style='{style}'>{status}</div>", unsafe_allow_html=True)
    else:
        st.markdown(f"<div style='{style}'>{'<br><br><br><br><br><br><br><br><br>'}</div>", unsafe_allow_html=True)

# view or delete channel from mongodb or mysql input
with colum2:
    st.markdown("<h3 style='text-align: left;color:rgb(138, 138, 138));'>Delete or Overview Channels</h3>", unsafe_allow_html=True)
    select_ms = st.radio("Select Database:", ("MongoDB", "MySQL"))
    select_op = st.radio("Select Operation:", ("View", "Delete"))
    input_ch=st.text_input("Enter channel ID to perform operation ")
    submitted = st.button("submit")

# view or delete channel from mongodb or mysql output
with colum3:
    if submitted:
        st.markdown("<h3 style='text-align: left; color:rgb(138, 138, 138);'>DB Operation Results</h3>", unsafe_allow_html=True)
        if input_ch:
            if select_ms=='MongoDB' and (select_op=='View' or select_op=='Delete'):
                query = {"Channel_ID": input_ch}
                complete_channel_info = collection.find(query)
                op_data=[]
                c_count=1
                ch_n=''
                for channel in complete_channel_info:
                    ch_n=channel['Channel_Name']
                    entry='Playlist '+str(c_count)+" "+channel['Playlist_Name'] + ", Videos:" +str(len(channel['Videos_Details']))
                    op_data.append(entry)
                    c_count+=1
                op_data='<br>'.join(op_data)
                if op_data:
                    st.write('#### Channel Name: '+ch_n)       
                    st.markdown(f"<div style='{style}'>{op_data}</div>", unsafe_allow_html=True)
                else:
                    st.write('#### Channel ID not present in MongoDB Database')   
            if select_ms=='MongoDB' and select_op=='Delete':
                filter = { 'Channel_ID': input_ch }
                result = collection.delete_many(filter)
                if result.deleted_count>0:
                    st.write('#### Channel '+ch_n+' and all its contents Deleted')     
            if select_ms=='MySQL' and (select_op=='View' or select_op=='Delete'):
                cursor.execute(f'SELECT Channel_Name FROM channels WHERE Channel_ID="{input_ch}"')
                result = cursor.fetchall()
                if cursor.rowcount>0:
                    nameofc=result[0][0]
                    cursor.execute(f'SELECT Playlist_Name FROM playlists WHERE Channel_ID="{input_ch}"')
                    result = cursor.fetchall()
                    op_data=[]
                    cq_count=1
                    for channel in result:
                        entry='Playlist '+str(cq_count)+": "+channel[0]
                        op_data.append(entry)
                        cq_count+=1
                    op_data='<br>'.join(op_data)
                    st.write('#### Channel Name: '+nameofc)       
                    st.markdown(f"<div style='{style}'>{op_data}</div>", unsafe_allow_html=True)
                else:
                    st.write("#### Channel ID not present in MySQL Database")   
            if select_ms=='MySQL' and select_op=='Delete':
                cursor.execute(f'DELETE FROM channels WHERE Channel_ID="{input_ch}"')
                if cursor.rowcount > 0:
                    st.write('#### Channel Name: '+nameofc+' and all its contents Deleted') 
        else:
            st.write('Please enter Channel ID')
    else:
        st.markdown("<h3 style='text-align: left; color:rgb(138, 138, 138)'>Database Operation Result will appear here</h3>", unsafe_allow_html=True)

#-------------------------------------------------SQL Queries--------------------------------------------

st.markdown("---")  
st.markdown("<h2 style='text-align: center; color:rgb(138, 138, 138);'>SQL Quries</h2>", unsafe_allow_html=True)
st.markdown("---")  
max_height = 700
style = f"max-height: {max_height}px; overflow-y: auto;"
c1, c2 = st.columns([6,4])

# show the available sql queries
with c1:
    st.markdown("<h3 style='text-align: center; color:rgb(138, 138, 138);'>SQL Queries Available</h3>", unsafe_allow_html=True)
    button1 = st.button("Query1: What are the names of all the videos and their corresponding channels?")
    button2 = st.button("Query2: Which channels have the most number of videos, and how many videos do they have?")
    button3 = st.button("Query3: What are the top 10 most viewed videos and their respective channels?")
    button4 = st.button("Query4: How many comments were made on each video, and what are their corresponding video names?")
    button5 = st.button("Query5: Which videos have the highest number of likes, and what are their corresponding channel names?")
    button6 = st.button("Query6: What is the total number of likes for each video, and what are their corresponding video names?")
    button7 = st.button("Query7: What is the total number of views for each channel, and what are their corresponding channel names?")
    button8 = st.button("Query8: What are the names of all the channels that have published videos in the year 2022?")
    button9 = st.button("Query9: What is the average duration of all videos in each channel, and their corresponding channel names?")
    button10 = st.button("Query10: Which videos have the highest number of comments, and what are their corresponding channel names?")

# show the result for selected sql query
with c2:
    if not button1 and not button2 and not button3 and not button4 and not button5 and not button6 and not button7 and not button8 and not button9 and not button10:
        st.markdown("<h3 style='text-align: center;color:rgb(138, 138, 138);'>SQL Queries Output will appear below</h3>", unsafe_allow_html=True)
    if button1:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51); '>Query1: Names of all the videos and their corresponding channels</h4>", unsafe_allow_html=True)
        cursor.execute('''
            SELECT videos.Video_Name, channels.Channel_Name
            FROM videos JOIN playlists ON videos.Playlist_Id = playlists.Playlist_Id
            JOIN channels ON playlists.Channel_ID = channels.Channel_ID
        ''')
        result = cursor.fetchall()
        video_names = []
        channel_names = []
        for row in result:
            video_name = row[0]
            channel_name = row[1]
            video_names.append(video_name)
            channel_names.append(channel_name)
        df = pd.DataFrame({
            'Video Name': video_names,
            'Channel Name': channel_names
        })
        desired_height = 480
        st.dataframe(df, height=desired_height)

    if button2:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query2: Top 5 channels which have the most number of videos </h4>", unsafe_allow_html=True)
        cursor.execute('''
            SELECT Channel_Name, Channel_Video_Count
            FROM channels
            ORDER BY Channel_Video_Count DESC
            LIMIT 5
        ''') 
        result = cursor.fetchall()
        channel_names = []
        video_counts = []
        for row in result:
            channel_name = row[0]
            video_count = row[1]
            channel_names.append(channel_name)
            video_counts.append(video_count)
        df = pd.DataFrame({
            'Channel Name': channel_names,
            'Video Count': video_counts
        })
        desired_height = 200
        st.dataframe(df, height=desired_height)

    if button3:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query3: Top 10 most viewed videos and their respective channels</h4>", unsafe_allow_html=True)
        cursor.execute('''
            SELECT videos.Video_Name, channels.Channel_Name
            FROM videos
            JOIN playlists ON videos.Playlist_Id = playlists.Playlist_Id
            JOIN channels ON playlists.Channel_ID = channels.Channel_ID
            ORDER BY videos.View_Count DESC
            LIMIT 10
        ''')
        result = cursor.fetchall()
        video_names = []
        channel_names = []
        for row in result:
            video_name = row[0]
            channel_name = row[1]
            video_names.append(video_name)
            channel_names.append(channel_name)
        df = pd.DataFrame({
            'Video Name': video_names,
            'Channel Name': channel_names
        })
        desired_height = 480
        st.dataframe(df, height=desired_height)
    
    if button4:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query4: Video names and their corresponding total comments</h4>", unsafe_allow_html=True)
        cursor.execute('''SELECT videos.Video_Name, COUNT(comments.Video_Id) AS repetition_count
                    FROM videos
                    JOIN comments ON videos.VP_Id = comments.Video_Id
                    GROUP BY videos.VP_Id
                ''')
        result = cursor.fetchall()
        video_names = []
        comment_counts = []
        for row in result:
            video_name = row[0]
            comment_count = row[1]
            video_names.append(video_name)
            comment_counts.append(comment_count)
        df = pd.DataFrame({
            'Video Name': video_names,
            'Comments Count': comment_counts
        })
        desired_height = 400
        st.dataframe(df, height=desired_height)

    if button5:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query5: Top 5 Videos with highest number of likes, and their corresponding channel names</h4>", unsafe_allow_html=True)
        cursor.execute('''SELECT videos.Video_Name, channels.Channel_Name
                FROM videos
                JOIN playlists ON videos.Playlist_Id = playlists.Playlist_Id
                JOIN channels ON playlists.Channel_ID = channels.Channel_ID
                ORDER BY videos.Like_Count DESC
                LIMIT 5
                ''')
        result = cursor.fetchall()
        video_names = []
        channel_names = []
        for row in result:
            video_name = row[0]
            channel_name = row[1]
            video_names.append(video_name)
            channel_names.append(channel_name)
        df = pd.DataFrame({
            'Video Name': video_names,
            'Channel Name': channel_names
        })
        desired_height = 400
        st.dataframe(df, height=desired_height)

    if button6:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query6: Total number of likes for each video, and their corresponding video names</h4>", unsafe_allow_html=True)
        cursor.execute('''
            SELECT videos.Video_Name, videos.Like_Count AS Total_Likes FROM videos
        ''')
        result = cursor.fetchall()
        video_names = []
        Like_Counts = []
        for row in result:
            video_name = row[0]
            like = row[1]
            video_names.append(video_name)
            Like_Counts.append(like)
        df = pd.DataFrame({
            'Video Name': video_names,
            'Like Count': Like_Counts
        })
        desired_height = 400
        st.dataframe(df, height=desired_height)

    if button7:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query7: Total number of views for each channel, and their corresponding channel names</h4>", unsafe_allow_html=True)
        cursor.execute('''SELECT channels.Channel_Views, channels.Channel_Name FROM channels
                ''')
        result = cursor.fetchall()
        Channel_Views = []
        Channel_Names = []
        for row in result:
            Channel_View = row[0]
            Channel_Name = row[1]
            Channel_Views.append(Channel_View)
            Channel_Names.append(Channel_Name)
        df = pd.DataFrame({
            'Channel Name': Channel_Names,
            'Channel Views': Channel_Views
        })
        desired_height = 400
        st.dataframe(df, height=desired_height)
    
    if button8:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query8: Names of all the channels that have published videos in the year 2022</h4>", unsafe_allow_html=True)
        cursor.execute('''
        SELECT c.Channel_Name,v.PublishedAt
        FROM channels c
        JOIN playlists p ON c.Channel_ID = p.Channel_ID
        JOIN videos v ON p.Playlist_Id = v.Playlist_Id
        ''')

        rows = cursor.fetchall()
        channels_2022=set()
        for row in rows:
            datetime_obj = datetime.strptime(str(row[1]), "%Y-%m-%d %H:%M:%S")
            year = datetime_obj.year
            #print(row[0],year)
            if year==2022:
                channels_2022.add(row[0])
        for channel in channels_2022:
            st.write(channel)

    if button9:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query9: Average duration of all videos in each channel, and their corresponding channel names</h4>", unsafe_allow_html=True)
        cursor.execute('''
        SELECT c.Channel_Name, v.Duration
        FROM channels c
        JOIN playlists p ON c.Channel_ID = p.Channel_ID
        JOIN videos v ON p.Playlist_Id = v.Playlist_Id
        ''')

        rows = cursor.fetchall()
        channels_2022 = set()
        names=[]
        durations=[]
        for row in rows:
            duration_str = str(row[1])
            duration_parts = duration_str[duration_str.find('T')+1:].split('H')

            minutes = 0
            if len(duration_parts) > 1:
                hours = int(duration_parts[0])
                minutes = int(duration_parts[1][:duration_parts[1].find('M')])
                minutes=hours*60+minutes
            else:
                minutes = int(duration_parts[0][:duration_parts[0].find('M')])
            names.append(row[0])
            durations.append(minutes)
        df = pd.DataFrame({
                    'Channel_Name': names,
                    'Average_video_duration': durations
                })
        average_duration = df.groupby('Channel_Name')['Average_video_duration'].mean()
        df_average_duration = pd.DataFrame(average_duration)
        desired_height = 400
        st.dataframe(df_average_duration, height=desired_height)
        
    if button10:
        st.markdown("<h4 style='text-align: center;color:rgb(255, 92, 51);'>Query10: Top 5 Videos have the highest number of comments, and their corresponding channel names</h4>", unsafe_allow_html=True)
        cursor.execute('''
        SELECT v.Video_Name, ch.Channel_Name, COUNT(c.Video_Id)
        FROM videos v
        JOIN comments c ON c.Video_Id = v.VP_Id
        JOIN playlists p ON p.Playlist_Id= v.Playlist_Id
        JOIN channels ch ON ch.Channel_ID = p.Channel_ID
        GROUP BY v.VP_Id
        ''')
        videos=[]
        channels=[]
        comments_count=[]
        rows = cursor.fetchall()
        for row in rows:
            videos.append(row[0])
            channels.append(row[1])
            comments_count.append(row[2])
        df = pd.DataFrame({
                    'Channel_Name': videos,
                    'Video_Name': channels,
                    'Comments_count':comments_count
                })
        desired_height = 300
        sorted_df = df.sort_values(by='Comments_count', ascending=False)
        st.dataframe(sorted_df[:5], height=desired_height)

client.close()
conn.commit()
conn.close()
