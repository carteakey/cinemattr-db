from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable
import os

os.environ["PINECONE_API_KEY"] = Variable.get("PINECONE_API_KEY")
os.environ["PINECONE_ENV"] = Variable.get("PINECONE_ENV")
os.environ["OPENAI_API_KEY"] = Variable.get("OPENAI_API_KEY")


@dag(
    dag_id="load_embeddings",
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["load", "cinemattr"],
    render_template_as_native_obj=True,
)
def taskflow():
    @task.virtualenv(
        task_id="load_embeddings",
        requirements=[
            "langchain",
            "duckdb==0.7.1",
            "lark",
            "tiktoken",
            "openai",
            "pinecone-client",
        ],
        system_site_packages=False,
    )
    def trigger():
        from langchain.embeddings import OpenAIEmbeddings
        from langchain.text_splitter import RecursiveCharacterTextSplitter
        from langchain.document_loaders import DuckDBLoader
        from langchain.vectorstores import Pinecone
        import os
        import pinecone
        import duckdb
        import tiktoken

        embeddings = OpenAIEmbeddings()

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

        encoding = tiktoken.encoding_for_model("gpt-3.5-turbo")

        def num_tokens_from_string(string: str, encoding_name: str) -> int:
            """Returns the number of tokens in a text string."""
            encoding = tiktoken.get_encoding(encoding_name)
            num_tokens = len(encoding.encode(string))
            return num_tokens

        texts = [d.page_content for d in docs]
        token_count = 0
        for text in texts:
            token_count += num_tokens_from_string(text, "cl100k_base")

        print(token_count)
        # $0.0001 / 1K tokens
        print("Est. Price:", str(token_count * 0.0001 / 1000))

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
        pinecone.create_index("cinemattr", dimension=1536)

        print("Loading vectors")
        vectorstore = Pinecone.from_documents(docs, embeddings, index_name="cinemattr")

    trigger()


dag = taskflow()
