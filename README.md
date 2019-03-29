# singer-runner

Singer Runner manages tap and target processes, as well as metrics, state, and configuration.

## Features
- Run a tap or target
- Pass run options via CLI paramters or JSON/YAML config file.
- Use local file system or S3 for piping the singer stream, storing state, and storing metrics.
- Metric storage, piping, and state storage can be extended / customized by inheriting from the base classes.

## Usage

Requires python 3, tested with python 3.7

### Install

```sh
git clone git@github.com:datamill-co/singer-runner.git
cd singer-runner
virtualenv -p python3 env
source env/bin/activate
pip install -e .
```

### Run

```sh
$ singer-runner 
Usage: singer-runner [OPTIONS] COMMAND [ARGS]...

Options:
  --help  Show this message and exit.

Commands:
  run-tap
  run-target
```

## Concepts

- Pipes
   - Pipes move a stream of [Singer messages](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#output) from tap to target. A pipe could be as simple as a local file, a file in S3, or Kafka.
- State Storage
	- Loads/saves the [state JSON](https://github.com/singer-io/getting-started/blob/master/docs/CONFIG_AND_STATE.md#state-file).
- Metrics Storage
   - Accepts a stream of [Singer Metrics](https://github.com/singer-io/getting-started/blob/master/docs/SYNC_MODE.md#metric-messages)

   
## Programmatic Usage

Singer runner can be used within any python application. The primary functions are in `singer_runner.runner` including:
- `run_tap` runs a tap
- `run_target` runs a target

Classes in the `singer_runner.metrics`, `singer_runner.pipes`, and `singer_runner.state` can be used as arguemnts, along with catalog/config.