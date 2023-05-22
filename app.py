from langchain.vectorstores import Pinecone
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.chat_models import ChatOpenAI
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain.chains.query_constructor.base import AttributeInfo
from langchain.callbacks import get_openai_callback
from langchain.cache import SQLiteCache
import langchain

import pinecone
import os
import json
import sys

model_name="sentence-transformers/all-mpnet-base-v2"
model_kwargs = {}
embeddings = HuggingFaceEmbeddings(model_name=model_name,cache_folder='/tmp',model_kwargs=model_kwargs)

metadata_field_info=[
    AttributeInfo(
        name="title",
        description="The title of the movie (in lowercase). Case sensitive", 
        type="string", 
    ),
    # AttributeInfo(
    #     name="description",
    #     description="The description of the movie (in lowercase)", 
    #     type="string", 
    # ),
    AttributeInfo(
        name="genre",
        description="The genres of the movie (in lowercase). Case sensitive", 
        type="string or list[string]", 
    ),
    # AttributeInfo(
    #     name="certificate",
    #     description="The certificate of the movie", 
    #     type="string", 
    # ),
    AttributeInfo(
        name="year",
        description="The year the movie was released. Only integers allowed", 
        type="integer", 
    ),
    AttributeInfo(
        name="stars",
        description="The name of the movie actors (in lowercase). Case sensitive", 
        type="string or list[string]", 
    ),
    AttributeInfo(
        name="directors",
        description="The name of the movie directors (in lowercase). Case sensitive", 
        type="string or list[string]", 
    ),
    AttributeInfo(
        name="runtime",
        description="The runtime of the movie in minutes", 
        type="string", 
    ),
    AttributeInfo(
        name="imdb_rating",
        description="A 1-10 rating for the movie on IMDB",
        type="float"
    ),
     AttributeInfo(
        name="ratingCount",
        description="How many people rated the movie on IMDB. Indicator of movie's popularity",
        type="integer"
    ),
]
document_content_description = "Summary and plot of the movie"

def getResults(inputQuery):
    pinecone.init(
        api_key=os.environ["PINECONE_API_KEY"],
        environment=os.environ["PINECONE_ENV"],
    )
    vectorstore = Pinecone.from_existing_index(
        os.environ["PINECONE_INDEX_NAME"], embeddings
    )
    #Cache results in a local SQLite database
    llm = ChatOpenAI(temperature=0, model_name='gpt-3.5-turbo')
    langchain.llm_cache = SQLiteCache(database_path="/tmp/langchain.db")

    retriever = SelfQueryRetriever.from_llm(
        llm,
        vectorstore,
        document_content_description,
        metadata_field_info,
        verbose=True,
    )

    with get_openai_callback() as cb:
        retriever.search_kwargs = {"k": 20}
        results = retriever.get_relevant_documents(inputQuery)
        titles = [result.metadata["source"]  for result in results]
        print(cb)
        return titles

def lambda_handler(event, context):
    
    # Parse the incoming event data
    print('queryStringParameters:', json.dumps(event['queryStringParameters']))
    query = event['queryStringParameters']['query']
    
    #lowercase
    query = query.lower()

    #remove special characters
    import re
    query = re.sub(r'[^0-9A-Za-z .-]', '', query)

    #remove full stop
    if query[-1]=='.':
        query=query[:-1]


    print('event:', json.dumps(event))
    print('Final query:',query)
    response = {}

    if query is not None:
        titles = getResults(query)
        
        # Prepare the response body
        response = {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'text/plain'
            },
            'body': json.dumps({'titles': titles})
        }
    
    return response
