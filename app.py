from langchain.vectorstores import Pinecone
from langchain.embeddings import HuggingFaceEmbeddings
from langchain.llms import OpenAI
from langchain.retrievers.self_query.base import SelfQueryRetriever
from langchain.chains.query_constructor.base import AttributeInfo
import pinecone
import os
import json
import sys

model_name="sentence-transformers/all-mpnet-base-v2"
model_kwargs = {}
embeddings = HuggingFaceEmbeddings(model_name=model_name, model_kwargs=model_kwargs)

metadata_field_info = [
    AttributeInfo(
        name="title",
        description="The title of the movie",
        type="string",
    ),
    AttributeInfo(
        name="description",
        description="The description of the movie",
        type="string",
    ),
    AttributeInfo(
        name="genre",
        description="The genre(s) of the movie",
        type="string or list[string]",
    ),
    AttributeInfo(
        name="certificate",
        description="The certificate of the movie",
        type="string",
    ),
    AttributeInfo(
        name="year",
        description="The year the movie was released",
        type="integer",
    ),
    AttributeInfo(
        name="stars",
        description="The name of the movie actors",
        type="string or list[string]",
    ),
    AttributeInfo(
        name="director",
        description="The name of the movie directors",
        type="string or list[string]",
    ),
    AttributeInfo(
        name="runtime",
        description="The runtime of the movie",
        type="string",
    ),
    AttributeInfo(
        name="imdb_rating",
        description="A 1-10 rating for the movie on IMDB",
        type="float",
    ),
    AttributeInfo(
        name="ratingCount",
        description="How many people rated the movie on IMDB. Indicator of movie's popularity",
        type="integer",
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
    llm = OpenAI(temperature=0.2)
    retriever = SelfQueryRetriever.from_llm(
        llm,
        vectorstore,
        document_content_description,
        metadata_field_info,
        verbose=True,
    )
    retriever.search_kwargs = {"k": 10}
    results = retriever.get_relevant_documents(inputQuery)

    titles = []
    for result in results:
        print(result)
        print(result.metadata["source"])
        titles.append(result.metadata["source"])

    return titles

def lambda_handler(event, context):
    # Parse the incoming event data
    print('queryStringParameters:', json.dumps(event['queryStringParameters']))
    query = event['queryStringParameters']['query']  
    print('event:', json.dumps(event))

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
