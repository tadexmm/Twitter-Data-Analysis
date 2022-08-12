import pandas as pd

class Clean_Tweets:
    """
    The PEP8 Standard AMAZING!!!
    """
    def __init__(self, df:pd.DataFrame):
        self.df = df
        print('Automation in Action...!!!')
        
    def drop_unwanted_column(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove rows that has column names. This error originated from
        the data collection stage.  
        """
        unwanted_rows = df[df['retweet_count'] == 'retweet_count' ].index
        df.drop(unwanted_rows , inplace=True)
        df = df[df['polarity'] != 'polarity']
        
        return df
    
    def drop_duplicate(self, df:pd.DataFrame)->pd.DataFrame:
        """
        drop duplicate rows
        """
        df.drop_duplicates()
        
        return df
    
    def convert_to_datetime(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert column to datetime
        """
        df["created_at"] = pd.to_datetime(df["created_at"], infer_datetime_format=True)
        df = df[df['created_at'] >= '2020-12-31' ]
        
        return df
    
    def convert_to_numbers(self, df:pd.DataFrame)->pd.DataFrame:
        """
        convert columns like polarity, subjectivity, retweet_count
        favorite_count etc to numbers
        """
        
        integer_columns = ["retweet_count", "followers_count", "friends_count", "user_mentions"]
        float_columns = ["polarity", "subjectivity"]
        
        df[integer_columns] = df[integer_columns].astype(int)
        df[float_columns] = df[float_columns].astype(float)
        
        return df
    
    def remove_non_english_tweets(self, df:pd.DataFrame)->pd.DataFrame:
        """
        remove non english tweets from lang
        """
        mask = df["lang"] == "en"
        df = df[mask]
        
        return df
    
    def remove_retweets(self, df:pd.DataFrame)->pd.DataFrame:
        mask = df["original_text"].str.contains("^RT")
        df = df[~mask]
        return df

    @classmethod
    def clean_df(_, df:pd.DataFrame)->pd.DataFrame:
        c = Clean_Tweets(df);
        df = c.drop_unwanted_column(df)
        df = c.drop_duplicate(df)
        df = c.convert_to_datetime(df)
        df = c.convert_to_numbers(df)
        df = c.remove_non_english_tweets(df)
        
        return df

