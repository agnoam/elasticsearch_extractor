import os
import argparse
from typing import Final
# from concurrent.futures import ThreadPoolExecutor

from elasticsearch import Elasticsearch

from models.arguments import Arguments

def get_scroll_id(client: Elasticsearch, index_name: str) -> str:
    '''
        Get scroll_id from elasticsearch index
        args:
            host: str - Elasticsearch host
            index_name: str - Target index
    '''
    res: dict = client.search(index=index_name, query={"match_all": {}}, scroll='10m')
    scroll_id: str = res['_scroll_id']

    return scroll_id

def data_handling(client: Elasticsearch, scroll_id: str) -> None:
    data = client.scroll(scroll_id=scroll_id)
    print('data=', data)
    pass

def main(args: Arguments) -> None:
    print('args are:', args)
    client = Elasticsearch('http://10.0.0.10:30000')
    
    # TODO: Remove this line
    scroll_id: Final[str] = get_scroll_id(client, 'kibana_sample_data_logs')
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