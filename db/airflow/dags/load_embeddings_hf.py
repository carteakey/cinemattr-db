from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable
import os

os.environ["PINECONE_API_KEY"] = Variable.get("PINECONE_API_KEY")
os.environ["PINECONE_ENV"] = Variable.get("PINECONE_ENV")
os.environ["OPENAI_API_KEY"] = Variable.get("OPENAI_API_KEY")
os.environ["HUGGINGFACEHUB_API_TOKEN"] = Variable.get("HUGGINGFACEHUB_API_TOKEN")


@dag(
    dag_id="load_embeddings_hf",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["load","cinemattr"],
    render_template_as_native_obj=True,
)
def taskflow():
    @task.virtualenv(
        task_id="load_embeddings_hf",
        requirements=[
            "langchain==0.0.197",
            "duckdb==0.7.1",
            "sentence-transformers",
            "lark",
            "openai",
            "pinecone-client",
        ],
        system_site_packages=False,
    )
    def trigger():
        from langchain.embeddings import OpenAIEmbeddings, HuggingFaceEmbeddings
        from langchain.text_splitter import RecursiveCharacterTextSplitter
        from langchain.document_loaders import DuckDBLoader
        from langchain.vectorstores import Pinecone
        import os
        import pinecone
        import duckdb

        print("Downloading HuggingFace Embeddings Model")
        model_name = "sentence-transformers/all-mpnet-base-v2"
        model_kwargs = {}
        embeddings = HuggingFaceEmbeddings(
            model_name=model_name,
            # cache_folder="/opt/airflow/tmp",
            model_kwargs=model_kwargs,
        )

        print("Loading duckdb movies")
        
        loader = DuckDBLoader(
        """SELECT
        title,
        stars,
        directors,
        year,
        genre,
        runtime,
        ratingCount,
        plot,
        summary,
        imdb_rating,
        source FROM movie_plots m""",
        database="/opt/airflow/db/db.duckdb",
        page_content_columns=["summary", "plot"],
        metadata_columns=[
            "source",
            "title",
            "stars",
            "directors",
            "year",
            "genre",
            "runtime",
            "imdb_rating",
            "ratingCount",
            ],
        )
      
        data = loader.load()
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=0)
        docs = text_splitter.split_documents(data)

        print("Total docs length", str(len(docs)))

        print("Initializing Pinecone")
        pinecone.init(
            api_key=os.environ["PINECONE_API_KEY"],
            environment=os.environ["PINECONE_ENV"],
        )

        print("Deleting existing index")
        pinecone.delete_index("cinemattr")

        print("Creating new index")
        pinecone.create_index("cinemattr", dimension=768)

        print("Loading vectors")
        vectorstore = Pinecone.from_documents(docs, embeddings, index_name="cinemattr")

    trigger()


dag = taskflow()
