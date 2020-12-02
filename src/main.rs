use clap;
use std::fs;
use std::time::{self};
use std::io::{self, Write};
mod indexers;
use indexers::*;

macro_rules! print_flush {
    ($($arg:tt),*) => {
        print!($($arg)*);
        io::stdout().flush().unwrap();
    }
}

fn main() {
    let matches = clap::App::new("fulltext")
                    .about("Dumb fulltext searcher")
                    .arg(clap::Arg::with_name("index")
                        .long("index")
                        .value_name("FILE")
                        .number_of_values(1)
                        .takes_value(true)
                        .multiple(true)
                        .required(true))
                    .arg(clap::Arg::with_name("index_threads")
                        .long("index_threads")
                        .value_name("NUM_THREADS")
                        .number_of_values(1)
                        .takes_value(true))
                    .arg(clap::Arg::with_name("parse_threads")
                        .long("parse_threads")
                        .value_name("NUM_THREADS")
                        .number_of_values(1)
                        .takes_value(true))
                    .arg(clap::Arg::with_name("backend")
                        .long("backend")
                        .value_name("BACKEND")
                        .number_of_values(1)
                        .default_value("rayon")
                        .possible_values(&["rayon", "threadpool"])
                        .takes_value(true))
                    .arg(clap::Arg::with_name("TERM")
                        .required(false)
                        .multiple(true))
                    .get_matches();
    
    let num_index_threads = match matches.value_of("index_threads") {
        Some(t) => t.parse::<usize>().unwrap(),
        None => num_cpus::get()
    };

    let num_parse_threads = match matches.value_of("parse_threads") {
        Some(t) => t.parse::<usize>().unwrap(),
        None => 6
    };

    let before_all = time::Instant::now();
    let index_filenames: Vec<&str> = matches.values_of("index").unwrap().collect();
    let mut file_contents = Vec::<String>::new();
    for filename in index_filenames {
        println!("Reading {}...", filename);
        let file_content: String = fs::read_to_string(filename).unwrap();
        file_contents.push(file_content);
    }
    let duration_read = time::Instant::now() - before_all;
    println!("Reading done. Elapsed: {} ms", duration_read.as_millis());

    let before_parse = time::Instant::now();
    let ref_contents: Vec<&str> = file_contents.iter().map(|s| s as &str).collect();
    let backend = matches.value_of("backend").unwrap();
    let mut word_index: Box<dyn DocumentIndexer> = match backend {
        "rayon" => Box::new(RayonIndexer::new()),
        "threadpool" => Box::new(ThreadPoolIndexer::new(num_parse_threads, num_index_threads)),
        _ => panic!("unknown backend")
    };

    println!("Building index using '{}' backend...", backend);
    word_index.build_index(ref_contents);
    let now = time::Instant::now();
    let duration_parse = now - before_parse;
    let duration_all = now - before_all;
    println!("Parsing and indexing elapsed: {} ms, Index size: {}, Num documents indexed: {}",
    duration_parse.as_millis(), word_index.num_tokens(), word_index.num_documents());
    println!("Total elapsed: {}", duration_all.as_millis());

    if let Some(terms) = matches.values_of("TERM") {
        let terms = terms.collect();
        let results = word_index.search(terms);
        for result in results {
            for doc in result.matches {
                println!("Found \"{}\" in {} {}", result.term, doc.title, doc.url);
            }
        }
    } else {
        loop {
            let mut input = String::new();
            print_flush!("Search: "); 
            match io::stdin().read_line(&mut input) {
                Ok(_) => {
                    let terms = input.split(' ').collect();
                    let before = time::Instant::now();
                    let results = word_index.search(terms);
                    let duration = time::Instant::now() - before;
                    println!("Search found {} results, completed in {} us", results.iter().map(|m| m.matches.len()).sum::<usize>(), duration.as_micros());
                    for result in results {
                        for doc in result.matches {
                            println!("Found \"{}\" in {} {}", result.term, doc.title, doc.url);
                        }
                    }              
                }
                Err(error) => println!("error: {}", error),
            }
        }
    }
}