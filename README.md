# geostorage

## Description

Geostorage is a Python-based project that aims to manage and manipulate distributed geographical data. It consists of multiple components including a client, manager, and worker, as well as ETL (Extract, Transform, Load) operations.

## Table of Contents

* [Installation](#installation)
* [Usage](#usage)
  * [Running the program](#running-the-program)
  * [Client](#client)
  * [Manager](#manager)
  * [Worker](#worker)
  * [ETL](#etl)
* [Dependencies](#dependencies)
* [Contributing](#contributing)
* [License](#license)

## Installation

Clone the repository and install the required packages.

```bash
git clone https://github.com/Sometimesfunny/geostorage.git
cd geostorage
pip install -r requirements.txt
```

## Usage

### Running the Program

You can run the entire program using `main.py`. This script initializes and runs all the other components (Client, Manager, Worker, and ETL).

```bash
python main.py
```

When you run `main.py`, you'll be prompted to choose the starter mode:

1. Run all instances
2. Run all, except client

Choose the appropriate option based on your needs.

### Client

The client is responsible for sending requests to the manager. You can run the client using the following command:

```python
python client.py
```

Here's a snippet from `client.py`:

```python
from mq_manager import MQManager
from constants import COMMAND_ALL_ROADS, COMMAND_QUIT, COMMAND_ROAD_BY_ID

class Client:
    def __init__(self) -> None:
        self.mq_manager = MQManager()
        self.mq_manager.add_queue("client")
        self.mq_manager.add_queue("manager")
    # ... (rest of the code)
```

### Manager

The manager handles incoming requests and forwards them to the appropriate worker. You can run the manager using the following command:

```python
python manager.py
```

### Worker

The worker performs the actual data processing tasks. You can run a worker using the following command:

```python
python worker.py
```

### ETL

The ETL component is responsible for extracting, transforming, and loading the data. You can run the ETL process using the following command:

```python
python etl.py
```

## Dependencies

The project requires the following dependencies:

* `pika`

You can install them using the `requirements.txt` file.

## Contributing

Feel free to contribute to this project by submitting pull requests or issues.

## License

This project is licensed under the MIT License.
