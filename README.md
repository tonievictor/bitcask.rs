# bitcask
A simple key-value store using the Bitcask log format for persistence.

## Installation
- Clone the repository and move into the directory
```bash
git clone https://github.com/tonievictor/bitcask.rs.git bitcask
```

## Usage
You can interact with the Bitcask store through a simple CLI.
```bash
cargo run <logdirectory>
```
> <logdirectory> is a path to a directory where log files will be stored, make
> sure to create this directory before starting the cli.

### Commands
- `put <key> <value>`: Stores a key-value pair.
- `get <key>`: Retrieves the value associated with a key.
- `remove <key>`: Removes a key (not yet implemented).
- `list keys`: List all keys in the datastore.
- `exit`: Sync the datastore and exit the program.

### Example
```bash

$ cargo run ./data
> put mykey myvalue
Successfully set key value pair

> get mykey
myvalue
```
