#!/usr/bin/env python3

import argparse
import gzip
import io
import json
import logging
import os
import subprocess
import sys
import copy

from typing import Dict, List, Optional
from joblib import Parallel, delayed, parallel_backend
from jsonschema import Draft7Validator, FormatChecker
from singer import get_logger
from datetime import datetime, timedelta, timezone

from target_snowflake.file_formats import csv
from target_snowflake.file_formats import parquet
from target_snowflake import stream_utils

from target_snowflake.db_sync import DbSync
from target_snowflake.file_format import FileFormatTypes
from target_snowflake.exceptions import (
    RecordValidationException,
    UnexpectedValueTypeException,
    InvalidValidationOperationException
)

LOGGER = get_logger('target_snowflake')

# Tone down snowflake.connector log noise by only outputting warnings and higher level messages
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

DEFAULT_BATCH_SIZE_ROWS = 100000
DEFAULT_PARALLELISM = 0  # 0 The number of threads used to flush tables
DEFAULT_MAX_PARALLELISM = 16  # Don't use more than this number of threads by default when flushing streams in parallel


def add_metadata_columns_to_schema(schema_message):
    """Metadata _sdc columns according to the stitch documentation at
    https://www.stitchdata.com/docs/data-structure/integration-schemas#sdc-columns

    Metadata columns gives information about data injections
    """
    extended_schema_message = schema_message
    extended_schema_message['schema']['properties']['_sdc_extracted_at'] = {'type': ['null', 'string'],
                                                                            'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_batched_at'] = {'type': ['null', 'string'],
                                                                          'format': 'date-time'}
    extended_schema_message['schema']['properties']['_sdc_deleted_at'] = {'type': ['null', 'string']}

    return extended_schema_message


def emit_state(state: Optional[Dict]):
    """Print state to stdout"""
    if state is not None:
        line = json.dumps(state)
        LOGGER.info('Emitting state %s', line)
        sys.stdout.write(f"{line}\n")
        sys.stdout.flush()


def get_snowflake_statics(config):
    """Retrieve common Snowflake items will be used multiple times

    Params:
        config: configuration dictionary

    Returns:
        tuple of retrieved items: table_cache, file_format_type
    """
    table_cache = []
    if not ('disable_table_cache' in config and config['disable_table_cache']):
        LOGGER.info('Getting catalog objects from table cache...')

        db = DbSync(config)  # pylint: disable=invalid-name
        table_cache = db.get_table_columns(
            table_schemas=stream_utils.get_schema_names_from_config(config))

    # The file format is detected at DbSync init time
    file_format_type = db.file_format.file_format_type

    return table_cache, file_format_type


# pylint: disable=too-many-locals,too-many-branches,too-many-statements,invalid-name
def persist_lines(config, lines, table_cache=None, file_format_type: FileFormatTypes = None) -> None:
    """Main loop to read and consume singer messages from stdin

    Params:
        config: configuration dictionary
        lines: iterable of singer messages
        table_cache: Optional dictionary of Snowflake table structures. This is useful to run the less
                     INFORMATION_SCHEMA and SHOW queries as possible.
                     If not provided then an SQL query will be generated at runtime to
                     get all the required information from Snowflake
        file_format_type: Optional FileFormatTypes value that defines which supported file format to use
                          to load data into Snowflake.
                          If not provided then it will be detected automatically

    Returns:
        tuple of retrieved items: table_cache, file_format_type
    """
    
    class Vals():
        state = None
        flushed_state = None
        schemas = {}
        key_properties = {}
        validators = {}
        records_to_load = {}
        row_count = {}
        stream_to_sync = {}
        total_row_count = {}
        batch_size_rows = config.get('batch_size_rows', DEFAULT_BATCH_SIZE_ROWS)
        batch_wait_limit_seconds = config.get('batch_wait_limit_seconds', None)
        flush_timestamp = datetime.now(timezone.utc)
        archive_load_files = config.get('archive_load_files', False)
        archive_load_files_data = {}

    v = Vals()

    def process_record(o: dict, v: Vals) -> None:
        """
        Process a single record, reflecting changes in state back to the vals
        """
        if 'stream' not in o:
            raise Exception(f"Line is missing required key 'stream': {line}")
        if o['stream'] not in v.schemas:
            raise Exception(
                f"A record for stream {o['stream']} was encountered before a corresponding schema")

        # Get schema for this record's stream
        stream = o['stream']

        stream_utils.adjust_timestamps_in_record(o['record'], v.schemas[stream])

        # Validate record
        if config.get('validate_records'):
            try:
                v.validators[stream].validate(stream_utils.float_to_decimal(o['record']))
            except Exception as ex:
                if type(ex).__name__ == "InvalidOperation":
                    raise InvalidValidationOperationException(
                        f"Data validation failed and cannot load to destination. RECORD: {o['record']}\n"
                        "multipleOf validations that allows long precisions are not supported (i.e. with 15 digits"
                        "or more) Try removing 'multipleOf' methods from JSON schema.") from ex
                raise RecordValidationException(f"Record does not pass schema validation. RECORD: {o['record']}") \
                    from ex

        primary_key_string = v.stream_to_sync[stream].record_primary_key_string(o['record'])
        if not primary_key_string:
            primary_key_string = f'RID-{v.total_row_count[stream]}'

        if stream not in v.records_to_load:
            v.records_to_load[stream] = {}

        # increment row count only when a new PK is encountered in the current batch
        if primary_key_string not in v.records_to_load[stream]:
            v.row_count[stream] += 1
            v.total_row_count[stream] += 1

        # append record
        if config.get('add_metadata_columns') or config.get('hard_delete'):
            v.records_to_load[stream][primary_key_string] = stream_utils.add_metadata_values_to_record(o)
        else:
            v.records_to_load[stream][primary_key_string] = o['record']

        if v.archive_load_files and stream in v.archive_load_files_data:
            # Keep track of min and max of the designated column
            stream_archive_load_files_values = v.archive_load_files_data[stream]
            if 'column' in stream_archive_load_files_values:
                incremental_key_column_name = stream_archive_load_files_values['column']
                incremental_key_value = o['record'][incremental_key_column_name]
                min_value = stream_archive_load_files_values['min']
                max_value = stream_archive_load_files_values['max']

                if min_value is None or min_value > incremental_key_value:
                    stream_archive_load_files_values['min'] = incremental_key_value

                if max_value is None or max_value < incremental_key_value:
                    stream_archive_load_files_values['max'] = incremental_key_value

        flush = False
        if v.row_count[stream] >= v.batch_size_rows:
            flush = True
            LOGGER.info("Flush triggered by batch_size_rows (%s) reached in %s",
                        v.batch_size_rows, stream)
        elif (v.batch_wait_limit_seconds and
            datetime.now(timezone.utc) >= (v.flush_timestamp + timedelta(seconds=v.batch_wait_limit_seconds))):
            flush = True
            LOGGER.info("Flush triggered by batch_wait_limit_seconds (%s)",
                        v.batch_wait_limit_seconds)

        if flush:
            # flush all streams, delete records if needed, reset counts and then emit current state
            if config.get('flush_all_streams'):
                filter_streams = None
            else:
                filter_streams = [stream]

            # Flush and return a new state dict with new positions only for the flushed streams
            v.flushed_state = flush_streams(
                v.records_to_load,
                v.row_count,
                v.stream_to_sync,
                config,
                v.state,
                v.flushed_state,
                v.archive_load_files_data,
                filter_streams=filter_streams)

            v.flush_timestamp = datetime.now(timezone.utc)

            # emit last encountered state
            emit_state(copy.deepcopy(v.flushed_state))

    # Loop over lines from stdin
    for line in lines:
        try:
            o = json.loads(line)
        except json.decoder.JSONDecodeError:
            LOGGER.error('Unable to parse:\n%s', line)
            raise

        if 'type' not in o:
            raise Exception(f"Line is missing required key 'type': {line}")

        t = o['type']

        if t in ('RECORD','BATCH'):
            if t == 'BATCH':
                """
                example of a batch message: 
                    {
                        "type": "BATCH",
                        "stream": "users",
                        "encoding": {
                            "format": "jsonl",
                            "compression": "gzip"
                        },
                        "manifest": [
                            "file://path/to/batch/file/1",
                            "file://path/to/batch/file/2"
                        ]
                    }
                """
                # loop over the lines of each JSONL FILE
                # for each record, create a fake record and process it like a normal record
                # only allowed file type is 'jsonl'
                # only allowed compression is 'gzip'
                assert o['encoding']['format'] == 'jsonl', f"BATCH encoding format must be 'jsonl', not {o['encoding']['format']}; RECORD: {o}"
                assert o['encoding']['compression'] == 'gzip', f"BATCH encoding compression must be 'gzip', not {o['encoding']['compression']}; RECORD: {o}"
                LOGGER.info(f"Received BATCH message: {o}")
                for file in o['manifest']:
                    LOGGER.info(f"Processing file from BATCH message: {file}")
                    # gunzip the local file
                    subprocess.run(['gunzip','-k',file.replace('file://','')])
                    # drop .gz suffix
                    filename = file.replace('file://','')[:-3]
                    with open(filename) as f:
                        for line in f:
                            
                            process_record({'stream':o['stream'],'record': json.loads(line),'time_extracted': datetime.now(timezone.utc)},v)
            else: 
                process_record(o,v)
        
        elif t == 'SCHEMA':
            if 'stream' not in o:
                raise Exception(f"Line is missing required key 'stream': {line}")

            stream = o['stream']
            new_schema = stream_utils.float_to_decimal(o['schema'])

            # Update and flush only if the the schema is new or different than
            # the previously used version of the schema
            if stream not in v.schemas or v.schemas[stream] != new_schema:

                v.schemas[stream] = new_schema
                v.validators[stream] = Draft7Validator(v.schemas[stream], format_checker=FormatChecker())

                # flush records from previous stream SCHEMA
                # if same stream has been encountered again, it means the schema might have been altered
                # so previous records need to be flushed
                if v.row_count.get(stream, 0) > 0:
                    # flush all streams, delete records if needed, reset counts and then emit current state
                    if config.get('flush_all_streams'):
                        filter_streams = None
                    else:
                        filter_streams = [stream]
                    v.flushed_state = flush_streams(v.records_to_load,
                                                  v.row_count,
                                                  v.stream_to_sync,
                                                  config,
                                                  v.state,
                                                  v.flushed_state,
                                                  v.archive_load_files_data,
                                                  filter_streams=filter_streams)

                    # emit latest encountered state
                    emit_state(v.flushed_state)

                # key_properties key must be available in the SCHEMA message.
                if 'key_properties' not in o:
                    raise Exception("key_properties field is required")

                # Log based and Incremental replications on tables with no Primary Key
                # cause duplicates when merging UPDATE events.
                # Stop loading data by default if no Primary Key.
                #
                # If you want to load tables with no Primary Key:
                #  1) Set ` 'primary_key_required': false ` in the target-snowflake config.json
                #  or
                #  2) Use fastsync [postgres-to-snowflake, mysql-to-snowflake, etc.]
                if config.get('primary_key_required', True) and len(o['key_properties']) == 0:
                    LOGGER.critical('Primary key is set to mandatory but not defined in the [%s] stream', stream)
                    raise Exception("key_properties field is required")

                v.key_properties[stream] = o['key_properties']

                if config.get('add_metadata_columns') or config.get('hard_delete'):
                    v.stream_to_sync[stream] = DbSync(config,
                                                    add_metadata_columns_to_schema(o),
                                                    table_cache,
                                                    file_format_type)
                else:
                    v.stream_to_sync[stream] = DbSync(config, o, table_cache, file_format_type)

                if v.archive_load_files:
                    v.archive_load_files_data[stream] = {
                        'tap': config.get('tap_id'),
                    }

                    # In case of incremental replication, track min/max of the replication key.
                    # Incremental replication is assumed if o['bookmark_properties'][0] is one of the columns.
                    incremental_key_column_name = stream_utils.get_incremental_key(o)
                    if incremental_key_column_name:
                        LOGGER.info("Using %s as incremental_key_column_name", incremental_key_column_name)
                        v.archive_load_files_data[stream].update(
                            column=incremental_key_column_name,
                            min=None,
                            max=None
                        )
                    else:
                        LOGGER.warning(
                            "archive_load_files is enabled, but no incremental_key_column_name was found. "
                            "Min/max values will not be added to metadata for stream %s.", stream
                        )

                v.stream_to_sync[stream].create_schema_if_not_exists()
                v.stream_to_sync[stream].sync_table()

                v.row_count[stream] = 0
                v.total_row_count[stream] = 0

        elif t == 'ACTIVATE_VERSION':
            LOGGER.debug('ACTIVATE_VERSION message')

        elif t == 'STATE':
            LOGGER.debug('Setting state to %s', o['value'])
            v.state = o['value']

            # # set flushed state if it's not defined or there are no records so far
            if not v.flushed_state or sum(v.row_count.values()) == 0:
                v.flushed_state = copy.deepcopy(v.state)

        else:
            raise Exception(f"Unknown message type {o['type']} in message {o}")

    # if some bucket has records that need to be flushed but haven't reached batch size
    # then flush all buckets.
    if sum(v.row_count.values()) > 0:
        # flush all streams one last time, delete records if needed, reset counts and then emit current state
        v.flushed_state = flush_streams(v.records_to_load, v.row_count, v.stream_to_sync, config, v.state, v.flushed_state,
                                      v.archive_load_files_data)

    # emit latest state
    emit_state(copy.deepcopy(v.flushed_state))


# pylint: disable=too-many-arguments
def flush_streams(
        streams,
        row_count,
        stream_to_sync,
        config,
        state,
        flushed_state,
        archive_load_files_data,
        filter_streams=None):
    """
    Flushes all buckets and resets records count to 0 as well as empties records to load list
    :param streams: dictionary with records to load per stream
    :param row_count: dictionary with row count per stream
    :param stream_to_sync: Snowflake db sync instance per stream
    :param config: dictionary containing the configuration
    :param state: dictionary containing the original state from tap
    :param flushed_state: dictionary containing updated states only when streams got flushed
    :param filter_streams: Keys of streams to flush from the streams dict. Default is every stream
    :param archive_load_files_data: dictionary of dictionaries containing archive load files data
    :return: State dict with flushed positions
    """
    parallelism = config.get("parallelism", DEFAULT_PARALLELISM)
    max_parallelism = config.get("max_parallelism", DEFAULT_MAX_PARALLELISM)

    # Parallelism 0 means auto parallelism:
    #
    # Auto parallelism trying to flush streams efficiently with auto defined number
    # of threads where the number of threads is the number of streams that need to
    # be loaded but it's not greater than the value of max_parallelism
    if parallelism == 0:
        n_streams_to_flush = len(streams.keys())
        if n_streams_to_flush > max_parallelism:
            parallelism = max_parallelism
        else:
            parallelism = n_streams_to_flush

    # Select the required streams to flush
    if filter_streams:
        streams_to_flush = filter_streams
    else:
        streams_to_flush = streams.keys()

    # Single-host, thread-based parallelism
    with parallel_backend('threading', n_jobs=parallelism):
        Parallel()(delayed(load_stream_batch)(
            stream=stream,
            records=streams[stream],
            row_count=row_count,
            db_sync=stream_to_sync[stream],
            no_compression=config.get('no_compression'),
            delete_rows=config.get('hard_delete'),
            temp_dir=config.get('temp_dir'),
            archive_load_files=copy.copy(archive_load_files_data.get(stream, None))
        ) for stream in streams_to_flush)

    # reset flushed stream records to empty to avoid flushing same records
    for stream in streams_to_flush:
        streams[stream] = {}

        # Update flushed streams
        if filter_streams:
            # update flushed_state position if we have state information for the stream
            if state is not None and stream in state.get('bookmarks', {}):
                # Create bookmark key if not exists
                if 'bookmarks' not in flushed_state:
                    flushed_state['bookmarks'] = {}
                # Copy the stream bookmark from the latest state
                flushed_state['bookmarks'][stream] = copy.deepcopy(state['bookmarks'][stream])

        # If we flush every bucket use the latest state
        else:
            flushed_state = copy.deepcopy(state)

        if stream in archive_load_files_data:
            archive_load_files_data[stream]['min'] = None
            archive_load_files_data[stream]['max'] = None

    # Return with state message with flushed positions
    return flushed_state


def load_stream_batch(stream, records, row_count, db_sync, no_compression=False, delete_rows=False,
                      temp_dir=None, archive_load_files=None):
    """Load one batch of the stream into target table"""
    # Load into snowflake
    if row_count[stream] > 0:
        flush_records(stream, records, db_sync, temp_dir, no_compression, archive_load_files)

        # Delete soft-deleted, flagged rows - where _sdc_deleted at is not null
        if delete_rows:
            db_sync.delete_rows(stream)

        # reset row count for the current stream
        row_count[stream] = 0


def flush_records(stream: str,
                  records: List[Dict],
                  db_sync: DbSync,
                  temp_dir: str = None,
                  no_compression: bool = False,
                  archive_load_files: Dict = None) -> None:
    """
    Takes a list of record messages and loads it into the snowflake target table

    Args:
        stream: Name of the stream
        records: List of dictionary, that represents multiple csv lines. Dict key is the column name, value is the
                 column value
        row_count:
        db_sync: A DbSync object
        temp_dir: Directory where intermediate temporary files will be created. (Default: OS specific temp directory)
        no_compression: Disable to use compressed files. (Default: False)
        archive_load_files: Data needed for archive load files. (Default: None)

    Returns:
        None
    """
    # Generate file on disk in the required format
    filepath = db_sync.file_format.formatter.records_to_file(records,
                                                             db_sync.flatten_schema,
                                                             compression=not no_compression,
                                                             dest_dir=temp_dir,
                                                             data_flattening_max_level=
                                                             db_sync.data_flattening_max_level)

    # Get file stats
    row_count = len(records)
    size_bytes = os.path.getsize(filepath)

    # Upload to s3 and load into Snowflake
    s3_key = db_sync.put_to_stage(filepath, stream, row_count, temp_dir=temp_dir)
    db_sync.load_file(s3_key, row_count, size_bytes)

    # Delete file from local disk
    os.remove(filepath)

    if archive_load_files:
        stream_name_parts = stream_utils.stream_name_to_dict(stream)
        if 'schema_name' not in stream_name_parts or 'table_name' not in stream_name_parts:
            raise Exception(f"Failed to extract schema and table names from stream '{stream}'")

        archive_schema = stream_name_parts['schema_name']
        archive_table = stream_name_parts['table_name']
        archive_tap = archive_load_files['tap']

        archive_metadata = {
            'tap': archive_tap,
            'schema': archive_schema,
            'table': archive_table,
            'archived-by': 'pipelinewise_target_snowflake'
        }

        if 'column' in archive_load_files:
            archive_metadata.update({
                'incremental-key': archive_load_files['column'],
                'incremental-key-min': str(archive_load_files['min']),
                'incremental-key-max': str(archive_load_files['max'])
            })

        # Use same file name as in import
        archive_file = os.path.basename(s3_key)
        archive_key = f"{archive_tap}/{archive_table}/{archive_file}"

        db_sync.copy_to_archive(s3_key, archive_key, archive_metadata)

    # Delete file from S3
    db_sync.delete_from_stage(stream, s3_key)


def main():
    """Main function"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', '--config', help='Config file')
    args = arg_parser.parse_args()

    if args.config:
        with open(args.config, encoding="utf8") as config_input:
            config = json.load(config_input)
    else:
        config = {}

    # Init columns cache
    table_cache, file_format_type = get_snowflake_statics(config)

    # Consume singer messages
    singer_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_lines(config, singer_messages, table_cache, file_format_type)

    LOGGER.debug("Exiting normally")


if __name__ == '__main__':
    main()
