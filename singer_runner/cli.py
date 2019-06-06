import sys
import json
import logging

import click
from yaml import load as yaml_load
try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader

from singer_runner.runner import run_tap, run_target
from singer_runner.state import FileStateStorage
from singer_runner.metrics import FileMetricsStorage
from singer_runner.pipes import FilePipe, GCSPipe, StdInOutPipe

CONFIG_STATE_STORAGE_CLASSES = {
    'file': FileStateStorage
}

CONFIG_METRICS_STORAGE_CLASSES = {
    'file': FileMetricsStorage
}

CONFIG_PIPE_CLASSES = {
    'file': FilePipe,
    'stdinout': StdInOutPipe,
    'gcs': GCSPipe
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

def init_module(runner_config, class_map, config_key):
    module_config = runner_config.get(config_key)
    if not module_config:
        return None
    module_class = class_map[module_config['type']]
    options = module_config['options']
    if isinstance(options, dict):
        return module_class(**options)
    else:
        return module_class(*options)

def init_modules(runner_config, is_tap):
    runner_modules = {}

    if is_tap:
        runner_modules['state_storage'] = init_module(runner_config,
                                                      CONFIG_STATE_STORAGE_CLASSES,
                                                      'tap_state')

    runner_modules['metrics_storage'] = init_module(runner_config,
                                                    CONFIG_METRICS_STORAGE_CLASSES,
                                                    'metrics')

    pipe = init_module(runner_config, CONFIG_PIPE_CLASSES, 'pipe')
    if not pipe:
        pipe = StdInOutPipe()
    runner_modules['pipe'] = pipe

    return runner_modules

def close_modules(runner_modules):
    for key, runner_module in runner_modules.items():
        if hasattr(runner_module, 'close'):
            runner_module.close()

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

    runner_modules = init_modules(runner_config, True)

    run_tap(logger,
            tap_command,
            tap_config_path=tap_config_path or runner_config.get('tap_config_path'),
            tap_state_path=tap_state_path or runner_config.get('tap_state_path'),
            tap_catalog_path=tap_catalog_path or runner_config.get('tap_catalog_path'),
            **runner_modules)

    close_modules(runner_modules)

@main.command('run-target')
@click.option('--runner-config-path', help='Path to singer-runner config')
@click.option('--target-command', help='Target command. Overrides singer-runner config')
@click.option('--target-config-path', help='Static target config file. Overrides singer-runner config')
def cli_run_target(runner_config_path,
                   target_command,
                   target_config_path):
    if not runner_config_path and not target_command:
        raise Exception('`--runner-config-path` or `--target-command` required.')

    logger = init_logger()

    runner_config = get_config(runner_config_path)

    target_command = target_command or runner_config.get('target_command')

    runner_modules = init_modules(runner_config, False)

    run_target(logger,
               target_command,
               target_config_path=target_config_path or runner_config.get('target_config_path'),
               **runner_modules)

    close_modules(runner_modules)
