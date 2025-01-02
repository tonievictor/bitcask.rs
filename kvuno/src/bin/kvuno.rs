use clap::Parser;
use std::process::exit;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// option - set | get | remove
    option: String,

    /// key
    #[arg(default_value_t = String::from(""))]
    key: String,

    /// value
    #[arg(default_value_t = String::from(""))]
    value: String
}

fn main() {
    let args = Args::parse();

    match args.option.as_str(){
        "set" => {
            eprintln!("unimplemented");
            exit(1);
        }
        "get" => {
            eprintln!("unimplemented");
            exit(1);
        }
        "rm" => {
            eprintln!("unimplemented");
            exit(1);
        }
 
        _ => {
            panic!("Unrecognized command");
        }
    }
}
