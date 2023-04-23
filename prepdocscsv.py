import argparse
import csv
import time
from azure.identity import DefaultAzureCredential
from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import *
from azure.search.documents import SearchClient
import codecs
from azure.search.documents import IndexDocumentsBatch

parser = argparse.ArgumentParser(
    description="Prepare documents by extracting content from a CSV file and indexing in a search index.",
    epilog="Example: prepdocs.py 'data.csv' --searchservice mysearch --index myindex -v"
)
parser.add_argument("files", help="Files to be processed")
parser.add_argument("--category", help="Value for the category field in the search index for all sections indexed in this run")
parser.add_argument("--skipblobs", action="store_true", help="Skip uploading individual pages to Azure Blob Storage")
parser.add_argument("--storageaccount", help="Azure Blob Storage account name")
parser.add_argument("--container", help="Azure Blob Storage container name")
parser.add_argument("--storagekey", required=False, help="Optional. Use this Azure Blob Storage account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--searchservice", help="Name of the Azure Cognitive Search service where content should be indexed (must exist already)")
parser.add_argument("--index", help="Name of the Azure Cognitive Search index where content should be indexed (will be created if it doesn't exist)")
parser.add_argument("--searchkey", required=False, help="Optional. Use this Azure Cognitive Search account key instead of the current user identity to login (use az login to set current user for Azure)")
parser.add_argument("--remove", action="store_true", help="Remove references to this document from blob storage and the search index")
parser.add_argument("--removeall", action="store_true", help="Remove all blobs from blob storage and documents from the search index")
parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
args = parser.parse_args()


# Use the current user identity to connect to Azure services unless a key is explicitly set for any of them
default_creds = DefaultAzureCredential() if args.searchkey == None else None
search_creds = default_creds if args.searchkey == None else AzureKeyCredential(args.searchkey)

def create_search_index():
    if args.verbose: print(f"Ensuring search index {args.index} exists")
    index_client = SearchIndexClient(endpoint=f"https://{args.searchservice}.search.windows.net/",
                                     credential=search_creds)
    if args.index not in index_client.list_index_names():
        index = SearchIndex(
            name=args.index,
            fields=[
                SimpleField(name="id", type="Edm.String", key=True),
                SearchableField(name="name", type="Edm.String", analyzer_name="en.microsoft"),
                SearchableField(name="description", type="Edm.String", analyzer_name="en.microsoft")
            ],
            semantic_settings=SemanticSettings(
                configurations=[SemanticConfiguration(
                    name='default',
                    prioritized_fields=PrioritizedFields(
                        title_field=None,
                        prioritized_content_fields=[
                            # CHANGES THESE TO FIT FIELD-NAMES THAT SUIT YOUR DATA 
                            SemanticField(field_name='name'),
                            SemanticField(field_name='description')
                        ]))
                ])
        )
        if args.verbose: print(f"Creating {args.index} search index")
        index_client.create_index(index)
    else:
        if args.verbose: print(f"Search index {args.index} already exists")

def index_sections(sections):
    if args.verbose: print(f"Indexing sections into search index '{args.index}'")
    search_client = SearchClient(endpoint=f"https://{args.searchservice}.search.windows.net/",
                                    index_name=args.index,
                                    credential=search_creds)
    batch = IndexDocumentsBatch()
    i = 0
    for s in sections:
        batch.add_upload_actions([s])
        i += 1
        if i % 1000 == 0:
            results = search_client.index_documents(batch=batch)
            succeeded = sum([1 for r in results if r.succeeded])
            if args.verbose: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")
            batch = IndexDocumentsBatch()

    if len(batch.actions) > 0:
        results = search_client.upload_documents(documents=batch)
        succeeded = sum([1 for r in results if r.succeeded])
    if args.verbose: print(f"\tIndexed {len(results)} sections, {succeeded} succeeded")

def remove_from_index():
    if args.verbose: print(f"Removing all documents from search index '{args.index}'")
    search_client = SearchClient(endpoint=f"https://{args.searchservice}.search.windows.net/",
                                            index_name=args.index,
                                            credential=search_creds)
    while True:
        r = search_client.search("", top=1000, include_total_count=True)
        if r.get_count() == 0:
            break
        r = search_client.delete_documents(documents=[{"id": d["id"]} for d in r])
        if args.verbose: print(f"\tRemoved {len(r)} sections from index")
        # It can take a few seconds for search results to reflect changes, so wait a bit
        time.sleep(2)

if args.removeall:
    remove_from_index()
else:
    if not args.remove:
        create_search_index()
    print(f"Processing CSV file...")
    with open(args.files, mode='r', encoding="utf-8-sig") as csvfile:
        csv_reader = csv.DictReader(csvfile)
        print("Header:", csv_reader.fieldnames)
        # CHANGE THESE TO BE SAME AS ROWS 54 AND 55
        sections = [{"id": row["id"], "name": row["name"], "description": row["description"]} for row in csv_reader]
        index_sections(sections)
