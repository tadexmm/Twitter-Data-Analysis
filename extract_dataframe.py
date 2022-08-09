import json
import pandas as pd
from textblob import TextBlob


def read_json(json_file: str)->list:
    """
    json file reader to open and read json files into a list
    Args:
    -----
    json_file: str - path of a json file
    
    Returns
    -------
    length of the json file and a list of json
    """
    
    tweets_data = []
    for tweets in open(json_file,'r'):
        tweets_data.append(json.loads(tweets))
    
    
    return len(tweets_data), tweets_data

class TweetDfExtractor:
    """
    this function will parse tweets json into a pandas dataframe
    
    Return
    ------
    dataframe
    """
    def __init__(self, tweets_list):
        
        self.tweets_list = tweets_list

    # an example function
    def find_statuses_count(self)->list:
        statuses_count = []
        for x in self.tweets_list:
            try:
                statuses_count.append(x['user']['statuses_count'])
            except KeyError:
                statuses_count.append(None)
            
        return statuses_count
        
    def find_full_text(self)->list:
        full_text = []
        for x in self.tweets_list:
            try:
                full_text.append(x['full_text'])
            except KeyError:
                full_text.append(None)
            
        return full_text
       
    
    def find_sentiments(self, text)->list:
        polarity = []
        subjectivity = []
        for x in text:
            t = TextBlob(x)
            polarity.append(t.polarity)
            subjectivity.append(t.subjectivity)
            
        return polarity, subjectivity

    def find_lang(self)->list:
        lang = []
        for x in self.tweets_list:
            try:
                lang.append(x['lang'])
            except KeyError:
                lang.append(None)
            
        return lang
        
    def find_created_time(self)->list:
        created_at = []
        for x in self.tweets_list:
            try:
                created_at.append(x['created_at'])
            except KeyError:
                created_at.append(None)
            
        return created_at

    def find_source(self)->list:
        source = []
        for x in self.tweets_list:
            try:
                source.append(x['source'])
            except KeyError:
                source.append(None)
            
        return source

    def find_screen_name(self)->list:
        screen_name = []
        for x in self.tweets_list:
            try:
                screen_name.append(x['user']['screen_name'])
            except KeyError:
                screen_name.append(None)
            
        return screen_name

    def find_followers_count(self)->list:
        followers_count = []
        for x in self.tweets_list:
            try:
                followers_count.append(x['user']['followers_count'])
            except KeyError:
                followers_count.append(None)
            
        return followers_count

    def find_friends_count(self)->list:
        friends_count = []
        for x in self.tweets_list:
            try:
                friends_count.append(x['user']['friends_count'])
            except KeyError:
                friends_count.append(None)
            
        return friends_count

    def is_sensitive(self)->list:
        is_sensitive = []
        for x in self.tweets_list:
            try:
                is_sensitive.append(x['possibly_sensitive'])
            except KeyError:
                is_sensitive.append(None)
            
        return is_sensitive

    def find_favourite_count(self)->list:
        favorite_count = []
        for x in self.tweets_list:
            try:
                favorite_count.append(x['favorite_count'])
            except KeyError:
                favorite_count.append(None)
            
        return favorite_count
    
    def find_retweet_count(self)->list:
        retweet_count = []
        for x in self.tweets_list:
            try:
                retweet_count.append(x['retweet_count'])
            except KeyError:
                retweet_count.append(None)
            
        return retweet_count

    def find_hashtags(self)->list:
        hashtags = []
        for x in self.tweets_list:
            try:
                hashtags.append([y["text"] for y in x['entities']['hashtags']])
            except KeyError:
                hashtags.append(None)
            
        return hashtags

    def find_mentions(self)->list:
        user_mentions = []
        for x in self.tweets_list:
            try:
                user_mentions.append(len(x['entities']['user_mentions']))
            except KeyError:
                user_mentions.append(None)
            
        return user_mentions


    def find_location(self)->list:
        location = []
        for x in self.tweets_list:
            try:
                location.append(x['user']['location'])
            except TypeError:
                location.append('')
            
        return location
        
    def get_tweet_df(self, save=False)->pd.DataFrame:
        """required column to be generated you should be creative and add more features"""
        
        columns = ['created_at', 'source', 'original_text','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
            'original_author', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place']
        
        created_at = self.find_created_time()
        source = self.find_source()
        text = self.find_full_text()
        polarity, subjectivity = self.find_sentiments(text)
        lang = self.find_lang()
        fav_count = self.find_favourite_count()
        retweet_count = self.find_retweet_count()
        screen_name = self.find_screen_name()
        follower_count = self.find_followers_count()
        friends_count = self.find_friends_count()
        sensitivity = self.is_sensitive()
        hashtags = self.find_hashtags()
        mentions = self.find_mentions()
        location = self.find_location()

        data = zip(created_at, source, text, polarity, subjectivity, lang, fav_count, retweet_count, screen_name, follower_count, friends_count, sensitivity, hashtags, mentions, location)
        df = pd.DataFrame(data=data, columns=columns)

        if save:
            df.to_csv('processed_tweet_data.csv', index=False)
            print('File Successfully Saved.!!!')
        
        return df

                
if __name__ == "__main__":
    # required column to be generated you should be creative and add more features
    columns = ['created_at', 'source', 'original_text','clean_text', 'sentiment','polarity','subjectivity', 'lang', 'favorite_count', 'retweet_count', 
    'original_author', 'screen_count', 'followers_count','friends_count','possibly_sensitive', 'hashtags', 'user_mentions', 'place', 'place_coord_boundaries']
    _, tweet_list = read_json("data/global_twitter_data.json")
    tweet = TweetDfExtractor(tweet_list)
    tweet_df = tweet.get_tweet_df() 

    # use all defined functions to generate a dataframe with the specified columns above
