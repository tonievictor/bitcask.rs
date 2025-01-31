# bitcask
A simple key-value store using the Bitcask log format for persistence.

## Installation
- Clone the repository and move into the directory
```bash
git clone https://github.com/tonievictor/distributed.systems.git distsys && cd distsys/bitcask
```

## Usage
You can interact with the Bitcask store through a simple CLI.
```bash
cargo run <logdirectory>
```
### Commands
- `set <key> <value>`: Stores a key-value pair.
- `get <key>`: Retrieves the value for a key.
- `remove <key>`: Removes a key (not yet implemented).
- `exit`: Exits the program.

### Example
```bash

$ cargo run ./data
> set mykey myvalue
Successfully set key value pair

> get mykey
myvalue
```
