use bitcask::Bitcask;
use std::env;
use std::io::stdin;
use std::path::Path;
use std::process::exit;
use ulid::Ulid;

#[derive(Debug)]
enum Action {
    Set,
    Get,
    Remove,
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
        let mut input = String::new();
        stdin()
            .read_line(&mut input)
            .expect("Failed to read user input");

        match parse_input(input) {
            Ok(command) => match command.action {
                Action::Set => match store.set(command.key.as_str(), command.value.as_str()) {
                    Ok(_) => println!("Successfully set key value pair"),
                    Err(err) => eprintln!("An error occured: {}", err),
                },
                Action::Get => match store.get(command.key) {
                    Ok(obj) => match obj {
                        Some(v) => println!("{}", v),
                        None => println!("Key does not exist in the database"),
                    },
                    Err(err) => eprintln!("An error occured: {}", err),
                },

                Action::Remove => unimplemented!(""),
            },
            Err(_) => continue,
        }
    }
}

fn parse_input(input: String) -> Result<Command, ()> {
    let newinput: Vec<_> = input.trim().split(" ").collect();

    let mut value = String::from("");

    let action = match newinput[0] {
        "set" => {
            if newinput.len() != 3 {
                eprintln!("Set command: set <key> <value>");
                return Err(());
            }
            value = String::from(newinput[2]);
            Action::Set
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
            exit(0);
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
    let dir = Path::new(dir_name);
    let filename = Ulid::new().to_string();
    let path = dir.join(Path::new(&filename));

    match Bitcask::open(path) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("An error occured while setting up the log, {:?}", err);
            exit(1)
        }
    }
}
