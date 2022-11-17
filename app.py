import os
import argparse
from typing import Final, Any
# from concurrent.futures import ThreadPoolExecutor

from elasticsearch import Elasticsearch
import pandas as pd

from models.arguments import Arguments

LAST_SCROLL_ID: str = None

def get_scroll_id(client: Elasticsearch, index_name: str, scroll_time: str='10m', batch_size: int=1000) -> str:
    '''
        Get scroll_id from elasticsearch index
        args:
            host: str - Elasticsearch host
            index_name: str - Target index
            scroll_time: str - How much time to save elasticsearch's scroll context (in minutes)
            batch_size: int - How much documents to retrive in each scroll
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

def data_handling(client: Elasticsearch, scroll_id: str, scroll_time: str='10m') -> None:
    '''
        Retrives the data from elasticsearch by scroll
        args:
            client: Elasticsearch - Elasticsearch client instance
            scroll_id: str - Current "page" scroll id
            scroll_time: str - How much time to save elasticsearch's scroll context (in minutes)
    '''
    data = client.scroll(scroll_id=scroll_id, scroll=scroll_time)
    print('total_hits in this scroll:', len(data['hits']['hits']))
    export_to_csv('', data['hits']['hits'])
    
    pass

def export_to_csv(saving_dir: str, data: list[dict[str, Any]]) -> None:
    df: pd.DataFrame = pd.json_normalize(data)
    # gen_file_name: str = f''

    # df.to_csv(os.path.join(saving_dir, 'dest.csv'))
    df.to_csv('dest.csv')



def main(args: Arguments) -> None:
    print('args are:', args)
    # TODO: Delete the hardcoded line below
    # client = Elasticsearch('http://10.0.0.10:30000')
    client = Elasticsearch(args.host or 'http://10.0.0.10:30000')

    
    # TODO: Delete the hardcoded line below
    # scroll_id: Final[str] = get_scroll_id(client, 'kibana_sample_data_logs')
    scroll_id: Final[str] = get_scroll_id(client, args.index)
    data_handling(client, scroll_id)
    # -----------------------------------------------------------------

    # Create thread per page and generate .csv file from it
    # thread_pool_executor = ThreadPoolExecutor(max_workers=args.max_workers)
    # thread_pool_executor.submit()

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
        '-mw', '--max-workers', default=10,
        help='The maximum thread workers to execute concurrently'
    )
    
    args: Arguments = parser.parse_args()
    main(args)