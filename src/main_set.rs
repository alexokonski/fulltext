use clap;

use std::fs;
use rust_stemmers;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io::{self, Write};
use std::time::{self};
use std::sync::mpsc::{self};
use num_cpus;
use crossbeam;

#[derive(Default)]
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

fn append_documents<'a, 'b>(file_contents: &'a str, docs: &'b mut Vec<Document<'a>>) {
    let mut cur_doc = Document::default();
    let mut cur_tag: &str = "";
    let mut id = 0;
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
                        docs.push(cur_doc);
                        cur_doc = Document::default();                        
                    }
                }
            },

            Ok(_) => {},
            Err(_) => {}
        }
    }
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

fn index_docs_thread(documents: &[Document], analyzer: &Analyzer) -> InvertedIndex {
    let mut inverted_index: InvertedIndex = InvertedIndex::with_capacity(500_000);
    
    for d in documents {
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

    inverted_index
}

fn index_all_docs(docs: &Vec<Document>, num_threads: i32, analyzer: &Analyzer) -> InvertedIndex {
    let num_per_thread = docs.len() / (num_threads as usize);
    if num_per_thread == 0 {
        return index_docs_thread(docs.as_slice(), analyzer);
    }

    crossbeam::thread::scope(|s| {
        let mut offset = 0;
        let (tx, rx) = mpsc::channel();
        for n in 0..num_threads {
            if offset >= docs.len() {
                break;
            }
            let mut num_for_thread = num_per_thread;
            if n == num_threads - 1 {
                num_for_thread = docs.len() - offset;
            }
            let thread_tx = tx.clone();
            s.spawn(move |_| {
                thread_tx.send(index_docs_thread(&docs[offset..offset+num_for_thread], analyzer)).unwrap();
            });
            offset += num_for_thread;
        }
        drop(tx);
        let mut rx_iter = rx.into_iter();
        let mut joined_index = rx_iter.next().unwrap();
        for mut thread_index in rx_iter {
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
    }).unwrap()
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
    let mut documents = Vec::<Document>::new();
    let mut file_contents = Vec::<String>::new();

    let before = time::Instant::now();
    for filename in index_filenames {
        println!("Reading {}...", filename);
        let file_content: String = fs::read_to_string(filename).unwrap();
        file_contents.push(file_content);
    }
    let duration = time::Instant::now() - before;
    println!("Reading Elapsed: {} ms", duration.as_millis());

    let before = time::Instant::now();
    let before_all = before;
    for contents in file_contents.iter() {
        println!("Parsing all files...");
        append_documents(contents, &mut documents);
    }
    let duration = time::Instant::now() - before;
    println!("Parsing Elapsed: {} ms", duration.as_millis());

    let analyzer: Analyzer = Analyzer::new_english();
    let num_threads = match matches.value_of("threads") {
        Some(t) => t.parse::<i32>().unwrap(),
        None => num_cpus::get() as i32
    };

    println!("Indexing with {} threads...", num_threads);
    let before = time::Instant::now();
    let inverted_index = index_all_docs(&documents, num_threads, &analyzer);
    let duration = time::Instant::now() - before;
    let duration_all = time::Instant::now() - before_all;
    println!("Indexing Elapsed: {} ms, Index size: {} terms", duration.as_millis(), inverted_index.len());
    println!("Parsing and Indexing Elapsed: {} ms", duration_all.as_millis());

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