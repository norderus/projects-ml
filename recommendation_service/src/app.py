import os
from typing import List
from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from schema import UserGet, PostGet, FeedGet
from datetime import datetime
from catboost import CatBoostClassifier
import pandas as pd
from loguru import logger
from typing import Optional
import json
import warnings
warnings.filterwarnings('ignore')


app = FastAPI()

# Get path to the model
def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # Cheking workspace of execution
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH


# Load model
def load_models():
    model_path = get_model_path("./catboost_model_main.cbm")  # Assuming the model file is in the same directory
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path)
    return model


# Load table with features from DB
def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "postgresql://{username}:{password}@{host}:{port}/{database}"
        "?sslmode=require"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)


# Query to the DB
def load_features(query) -> pd.DataFrame:
   return batch_load_sql(query)

#----------------------------------------------MAIN----------------------------------------------------------------
logger.info('Start downloading')
top_predictions = 5

posts_info = load_features('SELECT * FROM posts_info_features_default').drop('index', axis=1)
logger.info(posts_info.columns)
logger.info('posts_info_features_default is ready')

user_data = load_features('SELECT * FROM public.user_data')
logger.info('user_data is ready')

logger.info('Data is done')

model = load_models()
logger.info('Model is done')


#----------------------------------------------Endpoints-----------------------------------------------------------
@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(id: int, time: Optional[datetime] = None, limit: int = 5) -> List[PostGet]:
    if not time:
        time = datetime.now()

    # Collect data
    time_data = pd.DataFrame({'hour': [time.hour], 'month': [time.month]})
    user_info = user_data[user_data['user_id'] == id].drop('user_id', axis=1).reset_index(drop=True)
    time_data = pd.concat([time_data] * len(user_info), ignore_index=True)
    combined_data = pd.concat([time_data, user_info], axis=1)
    data = pd.merge(combined_data, posts_info, how='cross')
    

    # Make predictions
    predictions = model.predict_proba(data.drop(['text', 'post_id'], axis=1))
    data['predicted_proba'] = predictions[:, 1]
    data = data.sort_values(by='predicted_proba', ascending=False)
    top_5_posts = data.head(limit)[['post_id',  'text', 'topic']]
    recs = top_5_posts.rename(columns={'post_id': 'id'}).to_dict(orient='records')

    return recs