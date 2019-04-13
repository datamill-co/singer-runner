import sys
import json
import logging

import click
from yaml import load as yaml_load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from singer_runner.runner import run_tap
from singer_runner.state import FileStateStorage
from singer_runner.metrics import FileMetricsStorage
from singer_runner.pipes import FilePipe, StdInOutPipe

CONFIG_STATE_STORAGE_CLASSES = {
    'file': FileStateStorage
}

CONFIG_METRICS_STORAGE_CLASSES = {
    'file': FileMetricsStorage
}

CONFIG_PIPE_CLASSES = {
    'file': FilePipe
}

def get_config(config_path):
    if config_path is None:
        return {}

    with open(config_path) as file:
        ext = config_path.split('.')[-1]
        if ext == 'json':
            return json.load(file)
        elif ext == 'yaml':
            return yaml_load(file, Loader=Loader)
        else:
            raise Exception('`{}` not a support config file type'.format(ext))

def init_logger():
    logger = logging.getLogger()

    default_formatter = logging.Formatter('[%(asctime)s] %(name)s %(levelname)s %(message)s')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(default_formatter)
    logger.addHandler(stream_handler)

    logger.setLevel(logging.INFO)

    def exception_handler(type, value, tb):
        logger.exception("Uncaught exception: {}".format(str(value)),
                         exc_info=(type, value, tb))

    sys.excepthook = exception_handler

    return logging.getLogger('singer-runner')

@click.group()
def main():
    pass

@main.command('run-tap')
@click.option('--runner-config-path', help='Path to singer-runner config')
@click.option('--tap-command', help='Tap command. Overrides singer-runner config')
@click.option('--tap-config-path', help='Static tap config file. Overrides singer-runner config')
@click.option('--tap-state-path', help='Static tap state file. Overrides singer-runner config')
@click.option('--tap-catalog-path', help='Static tap catalog file. Overrides singer-runner config')
def cli_run_tap(runner_config_path,
                tap_command,
                tap_config_path,
                tap_state_path,
                tap_catalog_path):
    if not runner_config_path and not tap_command:
        raise Exception('`--runner-config-path` or `--tap-command` required.')

    logger = init_logger()

    runner_config = get_config(runner_config_path)

    tap_command = tap_command or runner_config.get('tap_command')

    state_storage = None
    state_storage_config = runner_config.get('tap_state')
    if state_storage_config:
        state_storage_class = CONFIG_STATE_STORAGE_CLASSES[state_storage_config['type']]
        options = state_storage_config['options']
        if isinstance(options, dict):
            state_storage = state_storage_class(**options)
        else:
            state_storage = state_storage_class(*options)

    metrics_storage = None
    metrics_storage_config = runner_config.get('metrics')
    if metrics_storage_config:
        metrics_storage_class = CONFIG_METRICS_STORAGE_CLASSES[metrics_storage_config['type']]
        options = metrics_storage_config['options']
        if isinstance(options, dict):
            metrics_storage = metrics_storage_class(**options)
        else:
            metrics_storage = metrics_storage_class(*options)

    pipe_config = runner_config.get('pipe')
    if pipe_config:
        pipe_class = CONFIG_PIPE_CLASSES[pipe_config['type']]
        options = pipe_config['options']
        if isinstance(options, dict):
            pipe = pipe_class(**options)
        else:
            pipe = pipe_class(*options)
    else:
        pipe = StdInOutPipe()

    run_tap(logger,
            tap_command,
            tap_config_path=tap_config_path or runner_config.get('tap_config_path'),
            tap_state_path=tap_state_path or runner_config.get('tap_state_path'),
            tap_catalog_path=tap_catalog_path or runner_config.get('tap_catalog_path'),
            state_storage=state_storage,
            metrics_storage=metrics_storage,
            pipe=pipe)
