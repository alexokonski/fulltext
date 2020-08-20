use clap;

use std::fs;
use rust_stemmers;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{self, Write};
use std::time::{self};
use num_cpus;
use crossbeam;
use crossbeam::crossbeam_channel;

#[derive(Default, Clone)]
struct Document<'a> {
    title: &'a str,
    url: &'a str,
    text: &'a str,
    id: i32
}

struct Analyzer<'a> {
    stopwords: HashSet<&'a str>,
    stemmer: rust_stemmers::Stemmer,
}

impl<'a> Analyzer<'a> {
    fn new_english() -> Analyzer<'a> {
        Analyzer { 
            stopwords: vec!["a", "and", "be", "have", "i", "in", "of", "that", "the", "to"].into_iter().collect(),
            stemmer: rust_stemmers::Stemmer::create(rust_stemmers::Algorithm::English)
        }
    }

    fn analyze(&self, letters: &str) -> Vec<String> {
        letters.split(|c: char| !c.is_alphanumeric())
            .map(|x| x.to_lowercase())
            .filter(|x| !self.stopwords.contains(x.as_str()) && !x.is_empty())
            .map(|x| self.stemmer.stem(&x).into_owned()).collect()
    }
}

type InvertedIndex = HashMap<String, HashSet<i32>>;

struct SearchResults<'a> {
    term: String,
    matches: Vec<&'a Document<'a>>,
}

fn append_documents<'a, 'b>(file_contents: &'a str, docs: &'b mut Vec<Document<'a>>, s: &crossbeam::channel::Sender<Vec<Document<'a>>>) {
    let mut cur_doc = Document::default();
    let mut cur_tag: &str = "";
    let mut id = 0;
    let chunk_size = 1000;
    let mut chunk: Vec<Document> = Vec::with_capacity(chunk_size);
    for token in xmlparser::Tokenizer::from(file_contents) {
        match token {
            Ok(xmlparser::Token::ElementStart{local, ..}) => {
                cur_tag = local.as_str();
            },
            Ok(xmlparser::Token::Text{text}) => {
                match cur_tag {
                    "title" => cur_doc.title = text.as_str(),
                    "abstract" => {
                        cur_doc.text = text.as_str()
                    },
                    "url" => cur_doc.url = text.as_str(),
                    _ => {}
                }
            },
            Ok(xmlparser::Token::ElementEnd{end, ..}) => {
                if let xmlparser::ElementEnd::Close(_, n) = end {
                    cur_tag = "";
                    if n.as_str() == "doc" {
                        cur_doc.id = id;
                        id += 1;
                        chunk.push(cur_doc.clone());
                        docs.push(cur_doc);
                    
                        if chunk.len() == chunk_size {
                            s.send(chunk).unwrap();
                            chunk = Vec::with_capacity(chunk_size);
                        }
                        cur_doc = Document::default();
                    }
                }
            },

            Ok(_) => {},
            Err(_) => {}
        }
    }
    s.send(chunk).unwrap();
}

fn search<'a>(docs: &'a Vec<Document>, index: &InvertedIndex, all_terms: Vec<&str>, analyzer: &Analyzer) -> Vec<SearchResults<'a>> {
    let mut results: Vec<SearchResults> = Vec::new();
    for search_term in all_terms {
        for term in analyzer.analyze(search_term) {
            if let Some(ids) = index.get(&term) {
                let mut matched_docs: Vec<&Document> = Vec::new();
                for id in ids {
                    matched_docs.push(&docs[*id as usize]);
                }
                results.push(SearchResults{term: term, matches: matched_docs});
            }
        }
    }

    results
}

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
                    .arg(clap::Arg::with_name("threads")
                        .long("threads")
                        .value_name("NUM_THREADS")
                        .number_of_values(1)
                        .takes_value(true))
                    .arg(clap::Arg::with_name("TERM")
                        .required(false)
                        .multiple(true))
                    .get_matches();
    
    let index_filenames: Vec<&str> = matches.values_of("index").unwrap().collect();
    let mut documents = Vec::<Document>::with_capacity(2_000_000);
    let mut file_contents = Vec::<String>::new();

    let before = time::Instant::now();

    for filename in index_filenames {
        println!("Reading {}...", filename);
        let file_content: String = fs::read_to_string(filename).unwrap();
        file_contents.push(file_content);
    }
    let duration = time::Instant::now() - before;
    println!("Reading Elapsed: {} ms", duration.as_millis());

    let num_threads = match matches.value_of("threads") {
        Some(t) => t.parse::<i32>().unwrap(),
        None => num_cpus::get() as i32
    };

    println!("Using {} threads to index", num_threads);
    let before_all = time::Instant::now();
    let analyzer: Analyzer = Analyzer::new_english();
    let inverted_index = crossbeam::thread::scope(|s| {
        let (tx_doc, rx_doc): (crossbeam_channel::Sender<Vec<Document>>, crossbeam_channel::Receiver<Vec<Document>>) = crossbeam_channel::unbounded();
        let (tx_index, rx_index) = crossbeam_channel::unbounded();
        for _ in 0..num_threads {
            let rx_doc = rx_doc.clone();
            let tx_index = tx_index.clone();
            let analyzer = &analyzer;
            s.spawn(move |_| {
                let mut inverted_index: InvertedIndex = InvertedIndex::with_capacity(500_000);
                for chunk in rx_doc {
                    for d in chunk {
                        for token in analyzer.analyze(&d.text) {
                            match inverted_index.get_mut(&token) {
                                Some(set) => {
                                    set.insert(d.id as i32);
                                }, 
                                None => {
                                    let mut set = HashSet::new();
                                    set.insert(d.id as i32);
                                    inverted_index.insert(token, set);
                                }
                            }
                        }
                    }
                }
                tx_index.send(inverted_index).unwrap();
                drop(tx_index);
            });
        }
        drop(tx_index);
        for contents in file_contents.iter() {
            append_documents(contents, &mut documents, &tx_doc);
        }
        drop(tx_doc);
        let mut rx_index_iter = rx_index.into_iter();
        let mut joined_index = rx_index_iter.next().unwrap();
        for mut thread_index in rx_index_iter {
            for (thread_k, thread_set) in thread_index.drain() {
                match joined_index.get_mut(&thread_k) {
                    Some(joined_set) => {
                        joined_set.extend(thread_set);
                    }, 
                    None => {
                        joined_index.insert(thread_k, thread_set);
                    }                 
                }
            }
        }
        joined_index
    }).unwrap();

    let duration_all = time::Instant::now() - before_all;
    println!("Parsing and indexing elapsed: {} ms, Index size: {}", duration_all.as_millis(), inverted_index.len());

    if let Some(terms) = matches.values_of("TERM")
    {
        let terms = terms.collect();
        let results = search(&documents, &inverted_index, terms, &analyzer);
        for result in results {
            for doc in result.matches {
                println!("Found \"{}\" in {} {}", result.term, doc.title, doc.url);
            }
        }
    }
    else
    {
        loop {
            let mut input = String::new();
            print_flush!("Search: "); 
            match io::stdin().read_line(&mut input) {
                Ok(_) => {
                    let terms = input.split(' ').collect();
                    let before = time::Instant::now();
                    let results = search(&documents, &inverted_index, terms, &analyzer);
                    let duration = time::Instant::now() - before;
                    println!("Search completed in {} us", duration.as_micros());
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