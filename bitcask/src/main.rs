use bitcask::Bitcask;
use clap::Parser;
use std::path::Path;
use std::process::exit;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// option - set | get | remove
    option: String,

    /// key
    #[arg(short, long)]
    key: String,

    /// value
    #[arg(short, long, default_value_t = String::from(""))]
    value: String,
}

fn main() {
    let args = Args::parse();
    let mut store = init_store("trial");
    match args.option.as_str() {
        "set" => match store.set(args.key.as_str(), args.value.as_str()) {
            Ok(_) => {
                println!("value is set");
                exit(0);
            }
            Err(err) => {
                eprintln!("Cannot set key: {:?}", err);
                exit(1);
            }
        },
        "get" => match store.get(args.key) {
            Ok(res) => match res {
                Some(val) => {
                    println!("{}", val);
                    exit(0)
                }
                None => {
                    eprintln!("Key does not exist");
                    exit(1);
                }
            },
            Err(err) => {
                eprintln!("An error occurred while getting the value {:?}", err);
                exit(1);
            }
        },
        _ => {
            println!("hello, who goes");
            exit(1);
        }
    }
}

fn init_store(dir_name: &str) -> Bitcask {
    let dir = Path::new(dir_name);
    let filename = String::from("log.btk");
    let path = dir.join(Path::new(&filename));

    match Bitcask::open(path) {
        Ok(store) => store,
        Err(err) => {
            eprintln!("An error occured while setting up the log, {:?}", err);
            exit(1)
        }
    }
}
