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

fn try_build_from_cache(matches: &clap::ArgMatches, word_index: &mut dyn DocumentIndexer, index_filename: &str) -> bool {
    if matches.is_present("no-cache-read") {
        return false;
    }

    let before = time::Instant::now();
    println!("Reading index files...");
    let load_result = SerializedIndex::load_from_path(index_filename);
    let duration = time::Instant::now() - before;
    println!("Reading complete. {} elapsed ms", duration.as_millis());
    match load_result {
        Ok(s) => {
            word_index.build_from_serialized(s);
            true
        }
        Err(_) => false
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
                        .required(true))
                    .arg(clap::Arg::with_name("index-threads")
                        .long("index-threads")
                        .value_name("NUM_THREADS")
                        .number_of_values(1)
                        .takes_value(true))
                    .arg(clap::Arg::with_name("parse-threads")
                        .long("parse-threads")
                        .value_name("NUM_THREADS")
                        .number_of_values(1)
                        .takes_value(true))
                    .arg(clap::Arg::with_name("backend")
                        .long("backend")
                        .value_name("BACKEND")
                        .number_of_values(1)
                        .default_value("rayon")
                        .possible_values(&["rayon", "threadpool", "threadpool_dashmap"])
                        .takes_value(true))
                    .arg(clap::Arg::with_name("no-cache-read")
                        .long("no-cache-read")
                        .help("don't use any on-disk cache, even if present"))
                    .arg(clap::Arg::with_name("no-cache-write")
                        .long("no-cache-write")
                        .help("don't write on-disk cache files after parsing"))
                    .arg(clap::Arg::with_name("TERM")
                        .required(false)
                        .multiple(true))
                    .get_matches();
    
    let num_index_threads = match matches.value_of("index-threads") {
        Some(t) => t.parse::<usize>().unwrap(),
        None => num_cpus::get()
    };

    let num_parse_threads = match matches.value_of("parse-threads") {
        Some(t) => t.parse::<usize>().unwrap(),
        None => 6
    };

    let backend = matches.value_of("backend").unwrap();

    let before_all = time::Instant::now();
    let index_filename = matches.value_of("index").unwrap();

    let before_parse = time::Instant::now();
    let mut word_index: Box<dyn DocumentIndexer> = match backend {
        "rayon" => Box::new(RayonIndexer::new()),
        "threadpool" => Box::new(ThreadPoolIndexer::new_hashmap(num_parse_threads, num_index_threads)),
        "threadpool_dashmap" => Box::new(ThreadPoolIndexer::new_dashmap(num_parse_threads, num_index_threads)),
        _ => panic!("unknown backend")
    };

    println!("Attempting to build from cache");
    let build_result = try_build_from_cache(&matches, word_index.as_mut(), index_filename);
    if build_result {
        println!("Build from cache successful!");
    } else {
        println!("Could not load from cache. Building index using '{}' backend...", backend);
        let file_content: String = fs::read_to_string(index_filename).unwrap();
        let duration_read = time::Instant::now() - before_all;
        println!("Reading done. Elapsed: {} ms", duration_read.as_millis());
        word_index.build_from_file_contents(file_content);
        let duration_parse = time::Instant::now() - before_parse;
        println!("Parsing and indexing elapsed: {} ms, Index size: {}, Num documents indexed: {}",
            duration_parse.as_millis(), word_index.num_tokens(), word_index.num_documents());
    }
    let duration_all = time::Instant::now() - before_all;
    println!("Total elapsed: {} ms", duration_all.as_millis());

    if !build_result && !matches.is_present("no-cache-write") {
        let before_write = time::Instant::now();
        let result = SerializedIndex::write_index_to_path(index_filename, word_index.as_ref());
        if result.is_err() {
            println!("Failed to write index: {:?}", result)
        }
        let duration_write = time::Instant::now() - before_write;
        println!("Duration write: {}", duration_write.as_millis());
    }

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