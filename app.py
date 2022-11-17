import os
import argparse
from typing import Any, Tuple
from concurrent.futures import ThreadPoolExecutor, wait
from time import sleep

from elasticsearch import Elasticsearch
import pandas as pd

from models.arguments import Arguments

IS_LAST: bool = False
LAST_SCROLL_ID: str = None
SCROLL_COUNT: int = 0

def get_scroll_id(client: Elasticsearch, index_name: str, scroll_time: str='10m', batch_size: int=1000) -> str:
    '''
        Get scroll_id from elasticsearch index
        args:
            host: str - Elasticsearch host
            index_name: str - Target index
            scroll_time: str - How much time to save elasticsearch's scroll context (in minutes)
            batch_size: int - How much documents to retrive in each scroll

        returns: Elasticsearch's scroll_id to run of
    '''
    page: dict = client.search(
        index=index_name,
        size=batch_size,
        scroll=scroll_time,
        query={
            "match_all": {}
        }
    )
    scroll_id: str = page['_scroll_id']

    return scroll_id

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
    df: pd.DataFrame = pd.json_normalize(data)
    df.to_csv(os.path.join(saving_dir, f'{index_name}.{file_count}.csv'))

def thread_runner(client: Elasticsearch, batch_size: int) -> None:
    global LAST_SCROLL_ID
    global IS_LAST
    global SCROLL_COUNT

    if not IS_LAST:
        next_scroll_id, all_data = data_handling(client, LAST_SCROLL_ID)
        export_to_csv(args.output, all_data, SCROLL_COUNT)

        # Updating the scroll id
        LAST_SCROLL_ID = next_scroll_id
        
        # Check whether is the last scroll
        IS_LAST = True if len(all_data) < batch_size else False
        
        # Update counter
        SCROLL_COUNT += 1

def main(args: Arguments) -> None:
    print('args are:', args)
    client = Elasticsearch(args.host or 'http://10.0.0.10:30000')
    print('Elasticsearch client created successfully')

    os.makedirs(args.output, exist_ok=True)
    print('Output directory created successfully')

    global LAST_SCROLL_ID
    LAST_SCROLL_ID = get_scroll_id(client, args.index) 

    # -----------------------------------------------------------------

    # Create thread per page and generate .csv file from it
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures: list = []

        while not IS_LAST:
            print('create new thread')
            future = executor.submit(thread_runner, client, args.batch_size)
            futures.append(future)
            
            sleep(.5)

        wait(futures)
        

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog = 'elasticsearch-exporter',
        description = 'Elasticsearch indices to csv exporter')

    parser.add_argument(
        '-i', '--index',
        help='The elasticsearch index to scroll of'
    ) # Will be required
    parser.add_argument(
        '-o', '--output',
        default=os.path.join(os.curdir, 'output'),
        help='Path of the directory to put all the .csv files'
    )
    parser.add_argument(
        '-eh', '--host',
        default='http://localhost:9200',
        help='The elasticsearch host to export the index from'
    )
    parser.add_argument(
        '-bs', '--batch-size',
        default=1000,
        help='How much documents to parse in each time'
    )
    parser.add_argument(
        '-mw', '--max-workers', default=10,
        help='The maximum thread workers to execute concurrently'
    )
    
    args: Arguments = parser.parse_args()
    main(args)