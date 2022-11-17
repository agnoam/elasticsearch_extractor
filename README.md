# Elasticsearch export
This is python script for exporting an index to a `.csv` file by multithreading.

## Process description
The script will connect to elasticsearch and start to "scroll" of the index. <br/>
Each iteration of the scroll will be saved as a single "split | chunk" file. (`.csv`) at the destination directory

## Supported arguments:
`--help` - Prints arguments help information.