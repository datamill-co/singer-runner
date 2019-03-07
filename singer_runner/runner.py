import json
from tempfile import NamedTemporaryFile

from singer_runner.process import SingerProcess
from singer_runner.pipes import FilePipe
from singer_runner.state import FileStateStorage
from singer_runner.utils import (
    run_thread,
    EOF,
    TAP,
    TARGET,
    create_queue
)

def create_temp_json_file(obj):
    file = NamedTemporaryFile(mode='w')
    json.dump(obj, file)
    file.flush()
    return file

def handle_file_option(command, temp_files, option, obj, file_path):
    if obj is not None and file_path is None:
        temp_file = create_temp_json_file(obj)
        file_path = temp_file.name
        temp_files.append(temp_file)

    if file_path is not None:
        command += [
            option,
            file_path
        ]

def run_tap(logger,
            tap_command,
            tap_config=None,
            tap_config_path=None,
            tap_state=None,
            tap_state_path=None,
            tap_catalog=None,
            tap_catalog_path=None,
            state_storage=None,
            metrics_storage=None,
            pipe=None):
    temp_files = []
    process = None

    command = [tap_command]

    handle_file_option(command,
                       temp_files,
                       '-c',
                       tap_config,
                       tap_config_path)

    if state_storage and \
        not tap_state and \
        not tap_state_path:
        state_storage.load()
        tap_state = state_storage.state

    handle_file_option(command,
                       temp_files,
                       '--state',
                       tap_state,
                       tap_state_path)

    handle_file_option(command,
                       temp_files,
                       '--catalog',
                       tap_catalog,
                       tap_catalog_path)

    handle_file_option(command,
                       temp_files,
                       '--properties',
                       tap_catalog,
                       tap_catalog_path)

    try:
        process = SingerProcess(logger,
                                command,
                                singer_process_type=TAP,
                                state_storage=state_storage,
                                metrics_storage=metrics_storage,
                                pipe=pipe)
        process.wait()
    except:
        if process:
            process.kill()
        raise
    finally:
        for temp_file in temp_files:
            temp_file.close()

def run_target(logger,
               target_command,
               target_config=None,
               target_config_path=None,
               metrics_storage=None,
               pipe=None):
    temp_files = []
    process = None

    command = [target_command]

    handle_file_option(command,
                       temp_files,
                       '-c',
                       target_config,
                       target_config_path)

    try:
        process = SingerProcess(logger,
                                command,
                                singer_process_type=TARGET,
                                metrics_storage=metrics_storage,
                                pipe=pipe)
        process.wait()
    except:
        if process:
            process.kill()
        raise
    finally:
        for temp_file in temp_files:
            temp_file.close()
