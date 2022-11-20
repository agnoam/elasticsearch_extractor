import os
import argparse
import time
from typing import Any, Tuple
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep

from elasticsearch import Elasticsearch
import pandas as pd

from models.arguments import Arguments

LAST_SCROLL_ID: str = None
SCROLL_COUNT: int = 0
DOCUMENT_NEEDED: int = -1
DOCUMENT_READ: int = -1

def get_scroll_id(client: Elasticsearch, index_name: str, scroll_time: str='10m', batch_size: int=1000) -> Tuple[int, str, list[dict[str, Any]]]:
    '''
        Get scroll_id from elasticsearch index
        args:
            host: str - Elasticsearch host
            index_name: str - Target index
            scroll_time: str - How much time to save elasticsearch's scroll context (in minutes)
            batch_size: int - How much documents to retrive in each scroll

        returns: 
            Tuple[0] - The number of total documents to retrive
            Tuple[1] - Elasticsearch's scroll_id to run of
            Tuple[2] - The data fetched from Elasticsearch
    '''
    page: dict = client.search(
        index=index_name,
        size=batch_size,
        scroll=scroll_time,
        query={
            "match_all": {}
        }
    )
    docs_to_read: int = page['hits']['total']['value']
    scroll_id: str = page['_scroll_id']
    data: list[dict[str, Any]] = page['hits']['hits']

    return docs_to_read, scroll_id, data

def data_handling(client: Elasticsearch, scroll_id: str, scroll_time: str='10m') -> Tuple[str, list[dict[str, Any]]]:
    '''
        Retrives the data from elasticsearch by scroll
        args:
            client: Elasticsearch - Elasticsearch client instance
            scroll_id: str - Current "page" scroll id
            scroll_time: str - How much time to save elasticsearch's scroll context (in minutes)

        returns:
            Tuple[0]: str - Next scroll id
            Tuple[1]: str - Current scroll data
    '''
    data = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
    print('total_hits in this scroll:', len(data['hits']['hits']))
    
    return data['_scroll_id'], data['hits']['hits']

def export_to_csv(saving_dir: str, data: list[dict[str, Any]], file_count: int, index_name: str='scrolled_index') -> None:
    '''
        Exporting the data found from the scroll
        args:
            saving_dir: str - Path to save the file to 
            data: list[dict[str, Any]] - The json array content to save as csv
            file_count: int - The number of the file (for the name)
            index_name: str - The index name for the file name
    '''
    global DOCUMENT_READ
    DOCUMENT_READ += len(data)

    df: pd.DataFrame = pd.json_normalize(data)
    df.to_csv(os.path.join(saving_dir, f'{index_name}.{file_count}.csv'))

def thread_runner(client: Elasticsearch, index: str, scroll_time: str, batch_size: int) -> None:
    '''
        Thread logic

        args:
            client: Elasticsearch - Elasticsearch client
            batch_size: int - Number of documents to process in one request (in scroll)
            scroll_time: str - How much time to save the scroll context
    '''

    global LAST_SCROLL_ID
    global DOCUMENT_READ
    global DOCUMENT_NEEDED
    global SCROLL_COUNT

    if not LAST_SCROLL_ID:
        DOCUMENT_NEEDED, LAST_SCROLL_ID, all_data = get_scroll_id(
            client, index, 
            scroll_time=scroll_time,
            batch_size=batch_size
        )
    else:
        next_scroll_id, all_data = data_handling(client, LAST_SCROLL_ID)
    
    if len(all_data) > 0:
        export_to_csv(args.output, all_data, SCROLL_COUNT)
        print('exported to .csv sucessfully')

        # Updating the scroll id
        LAST_SCROLL_ID = next_scroll_id

        # Update counter
        SCROLL_COUNT += 1

def main(args: Arguments) -> None:
    '''
        Main script function
        args:
            args: Arguments - all the arguments passed from the user
    '''
    starting_timestamp: int = time.time()
    print('args are:', args)
    client = Elasticsearch(args.host)
    print('Elasticsearch client created successfully')

    os.makedirs(args.output, exist_ok=True)
    print('Destenation folder verified')

    global LAST_SCROLL_ID
    global DOCUMENT_NEEDED
    global DOCUMENT_READ
    
    # Create thread per page and generate .csv file from it
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures: list = []

        while (DOCUMENT_READ < DOCUMENT_NEEDED) or not LAST_SCROLL_ID:
            print('Create new thread')
            future = executor.submit(
                thread_runner, client, 
                args.index, 
                scroll_time=args.scroll_time, 
                batch_size=args.batch_size
            )
            futures.append(future)
                
            sleep(args.thread_creation_sleep)
        
        wait(futures)
        print(f'''
            All process took: {(time.time() - starting_timestamp) / 1000}s 
            by batch_size of: {args.batch_size}
            thread-sleep: {args.thread_creation_sleep} minutes
            with threads running concurrently: {args.max_workers}
        ''')
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = 'elasticsearch-exporter',
        description = 'Elasticsearch indices to csv exporter')

    parser.add_argument(
        '-i', '--index',
        help='The elasticsearch index to scroll of',
        type=str
        # required=True
    ) # Will be required
    parser.add_argument(
        '-o', '--output',
        default=os.path.join(os.curdir, 'output'),
        type=str,
        help='Path of the directory to put all the .csv files'
    )
    parser.add_argument(
        '-eh', '--host',
        default='http://localhost:9200',
        type=str,
        help='The elasticsearch host to export the index from'
    )
    parser.add_argument(
        '-s', '--scroll-time',
        default='1m',
        type=str,
        help='How much time to save the scroll context (in elasticsearch)'
    )
    parser.add_argument(
        '-bs', '--batch-size',
        default=1000,
        type=int,
        help='How much documents to parse in each time'
    )
    parser.add_argument(
        '-mw', '--max-workers', default=10,
        type=int,
        help='The maximum thread workers to execute concurrently'
    )
    parser.add_argument(
        '-ts', '--thread-creation-sleep',
        type=float,
        default=0.5,
        help='How much time to wait between threads creation (in minutes)'
    )
    
    args: Arguments = parser.parse_args()
    main(args)