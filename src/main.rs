use bitcask::Bitcask;
use std::env;
use std::io::{stdin, stdout, Write};
use std::path::Path;
use std::process::exit;

#[derive(Debug)]
enum Action {
    Put,
    Get,
    Remove,
    Exit,
    ListKeys,
}

#[derive(Debug)]
struct Command {
    action: Action,
    key: String,
    value: String,
}

fn main() {
    let args: Vec<_> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage {} <logdirectory>", args[0]);
        exit(1);
    }
    let mut store = init_store(&args[1]);

    loop {
        print!("> ");
        stdout().flush().expect("Failed to flush stdout");

        let mut input = String::new();
        stdin()
            .read_line(&mut input)
            .expect("Failed to read user input");

        match parse_input(input) {
            Ok(command) => match command.action {
                Action::Put => match store.put(command.key.as_str(), command.value.as_str()) {
                    Ok(_) => println!("Successfully set key value pair"),
                    Err(err) => eprintln!("An error occured: {err}"),
                },
                Action::Get => match store.get(command.key) {
                    Ok(obj) => match obj {
                        Some(v) => println!("{v}"),
                        None => eprintln!("Key does not exist in the database"),
                    },
                    Err(err) => eprintln!("An error occured: {err}"),
                },
                Action::Exit => match store.close() {
                    Ok(_) => {
                        println!("Bye for now...");
                        exit(0);
                    }
                    Err(err) => {
                        eprintln!("could not exit: {err}");
                        eprintln!("An error occured while syncing datastore, try again");
                    }
                },
                Action::ListKeys => println!("Keys: {:?}", store.list_keys()),
                Action::Remove => match store.remove(command.key.as_str()) {
                    Ok(_) => println!("Deleted key from database"),
                    Err(err) => {
                        eprintln!("{err}");
                    }
                },
            },
            Err(_) => continue,
        }
    }
}

fn parse_input(input: String) -> Result<Command, ()> {
    let newinput: Vec<_> = input.trim().split(" ").collect();

    let mut value = String::from("");

    let action = match newinput[0] {
        "put" => {
            if newinput.len() != 3 {
                eprintln!("Put command: put <key> <value>");
                return Err(());
            }
            value = String::from(newinput[2]);
            Action::Put
        }
        "get" => {
            if newinput.len() != 2 {
                eprintln!("Get command: get <key>");
                return Err(());
            }
            Action::Get
        }
        "remove" => {
            if newinput.len() != 2 {
                eprintln!("Remove command: remove <key>");
                return Err(());
            }
            Action::Remove
        }
        "exit" => {
            if newinput.len() != 1 {
                eprintln!("exit requires no arguments!!!");
                return Err(());
            }
            return Ok(Command {
                action: Action::Exit,
                key: "".to_string(),
                value: "".to_string(),
            });
        }
        "list" => {
            if newinput.len() != 2 || newinput[1] != "keys" {
                eprintln!("hint: list keys");
                return Err(());
            }
            Action::ListKeys
        }
        _ => {
            println!("invalid command");
            return Err(());
        }
    };

    Ok(Command {
        action,
        key: String::from(newinput[1]),
        value,
    })
}

fn init_store(dir_name: &str) -> Bitcask {
    match Bitcask::open(Path::new(dir_name)) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("An error occured while setting up the log, {err:?}");
            exit(1)
        }
    }
}
