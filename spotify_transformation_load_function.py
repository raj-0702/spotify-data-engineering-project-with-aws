import json
import boto3
import pandas as pd
from datetime import datetime
from io import StringIO

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    Bucket = 'spotify-etl-project-raj'
    Key = 'raw_data/to_processed/'
    
    def album(data):
        album_list = []
        for row in data['items']:
            album_id = row['track']['album']['id']
            album_name = row['track']['album']['name']
            album_dor = row['track']['album']['release_date']
            album_link = row['track']['album']['external_urls']['spotify']
            album_total_track = row['track']['album']['total_tracks']
            album_dict = {'album_id':album_id,'name':album_name,'release_date':album_dor,'url':album_link,'total_track':album_total_track}
            album_list.append(album_dict)
        return album_list
        
    def artist(data):
        artist_list = []
        for tracks in data['items']:
            for row in tracks['track']['artists']:
                artist_id = row['id']
                artist_name = row['name']
                href = row['href']
                artist_element = {'artist_id' : artist_id,'artist_name':artist_name,'artist_url':href}
                artist_list.append(artist_element)
        return artist_list
        
    def songs(data):
        song_list = []
        for row in data['items']:
            song_id = row['track']['id']
            song_name = row['track']['name']
            song_duration = row['track']['duration_ms']
            song_url = row['track']['external_urls']['spotify']
            song_popularity = row['track']['popularity']
            song_added = row['added_at']
            album_id = row['track']['album']['id']
            artist_id = row['track']['artists'][0]['id']
            song_element = {'song_id':song_id,'song_name':song_name,'duration_ms':song_duration,'url':song_url,'popularity':song_popularity,
                           'song_added':song_added,'album_id':album_id,'artist_id':artist_id}
            song_list.append(song_element)
        return song_list
    
    
    spotify_data = []
    spotify_keys = []
    for row in s3.list_objects(Bucket = Bucket,Prefix = Key)['Contents']:
        file_key = row['Key']
        
        if file_key.split('.')[-1] == 'json':
            response = s3.get_object( Bucket = Bucket , Key = file_key)
            content = response['Body']
            jsonObject = json.loads(content.read())
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)
    
    
    for data in spotify_data:
        
        album_df = pd.DataFrame.from_dict(album(data))
        artist_df = pd.DataFrame.from_dict(artist(data))
        songs_df = pd.DataFrame.from_dict(songs(data))
        
        album_df = album_df.drop_duplicates(subset = ['album_id'])
        artist_df = artist_df.drop_duplicates(subset = ['artist_id'])
        songs_df = songs_df.drop_duplicates(subset = ['song_id'])
        
        album_df['release_date']=pd.to_datetime(album_df['release_date'])
        songs_df['song_added']=pd.to_datetime(songs_df['song_added'])
        
        album_key = 'transformed_data/album_data/album_transformed_' + str(datetime.now()) + '.csv'
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key = album_key ,Body = album_content)
        
        artist_key = 'transformed_data/artist_data/artist_transformed_' + str(datetime.now()) + '.csv'
        artist_buffer = StringIO()
        artist_df.to_csv(artist_buffer, index = False)
        artist_content = artist_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key = artist_key ,Body = artist_content)
        
        songs_key = 'transformed_data/songs_data/songs_transformed_' + str(datetime.now()) + '.csv'
        songs_buffer = StringIO()
        songs_df.to_csv(songs_buffer, index = False)
        songs_content = songs_buffer.getvalue()
        s3.put_object(Bucket=Bucket,Key = songs_key ,Body = songs_content)
        
    s3_resource = boto3.resource('s3')
    for keys in spotify_keys:
        copy_source = {
            'Bucket' : Bucket,
            'Key' : keys
        }
        s3_resource.meta.client.copy(copy_source,Bucket,Key = 'raw_data/processed/' + keys.split('/')[-1])
        s3_resource.Object(Bucket,keys).delete()
        
        
        
        
        
        
        
        
        
        
            
    
