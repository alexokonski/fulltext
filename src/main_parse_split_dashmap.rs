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
use rayon;
use std::hash::{Hash, Hasher};
use core::ops::Range;
use std::sync::atomic;
use std::cmp;
use dashmap;

static CUR_ID: atomic::AtomicI32 = atomic::AtomicI32::new(0);

#[derive(Default, Clone)]
struct Document<'a> {
    title: &'a str,
    url: &'a str,
    text: &'a str,
    id: i32
}

impl<'a> PartialEq for Document<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl<'a> Eq for Document<'a> {}

impl<'a> PartialOrd for Document<'a> {
    fn partial_cmp(&self, other: &Document) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for Document<'a> {
    fn cmp(&self, other: &Document) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl<'a> Hash for Document<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id.hash(state);
    }
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

type InvertedIndex = dashmap::DashMap<String, dashmap::DashSet<i32>>;
type DocumentSender<'a> = crossbeam_channel::Sender<Vec<Document<'a>>>;
type DocumentReceiver<'a> = crossbeam_channel::Receiver<Vec<Document<'a>>>;
//type IndexSender = crossbeam_channel::Sender<InvertedIndex>;
type IndexReceiver = crossbeam_channel::Receiver<InvertedIndex>;

type DocumentIndex<'a> = Vec<Document<'a>>;
type AllDocSender<'a> = crossbeam_channel::Sender<DocumentIndex<'a>>;
type AllDocReceiver<'a> = crossbeam_channel::Receiver<DocumentIndex<'a>>;


struct SearchResults<'a> {
    term: String,
    matches: Vec<&'a Document<'a>>,
}

fn parse_task<'a>(contents: &'a str, tx_doc: DocumentSender<'a>, tx_alldocs: AllDocSender<'a>) {
    let mut cur_doc = Document::default();
    let mut cur_tag: &str = "";
    let chunk_size = 100;
    let mut chunk: Vec<Document> = Vec::with_capacity(chunk_size);
    let mut all_docs: DocumentIndex = Vec::with_capacity(2_000_000);

    for token in xmlparser::Tokenizer::from_fragment(contents, Range{start: 0, end: contents.len()}) {
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
                        cur_doc.id = CUR_ID.fetch_add(1, atomic::Ordering::SeqCst);
                        chunk.push(cur_doc.clone());
                        all_docs.push(cur_doc);
                    
                        if chunk.len() == chunk_size {
                            tx_doc.send(chunk).unwrap();
                            chunk = Vec::with_capacity(chunk_size);
                        }
                        cur_doc = Document::default();
                    }
                }
            },

            Ok(_) => {},
            Err(_) => { println!("ERROR!"); }
        }
    }
    println!("Parse task complete");
    tx_doc.send(chunk).unwrap();
    tx_alldocs.send(all_docs).unwrap();
}

fn parse_documents<'a>(file_contents: &'a Vec<&str>, scope: &rayon::Scope<'a>, tx_doc: DocumentSender<'a>) -> AllDocReceiver<'a> {
    let (tx_alldocs, rx_alldocs): (AllDocSender, AllDocReceiver) = crossbeam_channel::unbounded();
    for contents in file_contents {
        let tx_doc = tx_doc.clone();
        let tx_alldocs = tx_alldocs.clone();
        scope.spawn(move |_| {
            parse_task(contents, tx_doc, tx_alldocs)
        });    
    }
    rx_alldocs
}

fn index_task(rx_doc: DocumentReceiver, inverted_index: &InvertedIndex, analyzer: &Analyzer) {
    //let mut inverted_index: InvertedIndex = InvertedIndex::with_capacity(500_000);
    for chunk in rx_doc {
        for d in chunk {
            for token in analyzer.analyze(&d.text) {
                match inverted_index.get_mut(&token) {
                    Some(set) => {
                        set.insert(d.id as i32);
                    }, 
                    None => {
                        let set = dashmap::DashSet::new();
                        set.insert(d.id as i32);
                        inverted_index.insert(token, set);
                    }
                }
            }
        }
    }
    //tx_index.send(inverted_index).unwrap();
}

fn spawn_index_tasks<'a>(num_threads: i32, scope: &rayon::Scope<'a>, inverted_index: &'a InvertedIndex, analyzer: &'a Analyzer) -> (DocumentSender<'a>, IndexReceiver) {
    let (tx_doc, rx_doc): (DocumentSender<'a>, DocumentReceiver<'a>) = crossbeam_channel::unbounded();
    let (tx_index, rx_index) = crossbeam_channel::unbounded();
    for _ in 0..num_threads {
        let rx_doc = rx_doc.clone();
        scope.spawn(move |_| {
            index_task(rx_doc, inverted_index, analyzer)
        });
    }
    (tx_doc, rx_index)
}

fn search<'a>(docs: &'a DocumentIndex, index: &InvertedIndex, all_terms: Vec<&str>, analyzer: &Analyzer) -> Vec<SearchResults<'a>> {
    let mut results: Vec<SearchResults> = Vec::new();
    for search_term in all_terms {
        for term in analyzer.analyze(search_term) {
            if let Some(ids) = index.get(&term) {
                let mut matched_docs: Vec<&Document> = Vec::new();
                for id in ids.iter() {
                    matched_docs.push(&docs[*id as usize]);
                }
                results.push(SearchResults{term: term, matches: matched_docs});
            }
        }
    }

    results
}

fn split_contents<'a>(contents: &'a str, split_on_tag: &str, num_chunks: usize) -> Vec<&'a str> {
    assert!(num_chunks > 0);
    if num_chunks <= 1 {
        return vec![contents];
    }
    let chars_per_split = contents.len() / num_chunks;
    let mut prev_index = 0;
    let mut splits: Vec<&str> = Vec::new();
    for _ in 0..num_chunks {
        let try_index = prev_index + chars_per_split;
        if try_index >= contents.len() {
            splits.push(&contents[prev_index..]);
            break;
        }
        let ending_index;
        match &contents[try_index..].find(split_on_tag) {
            Some(index) => {
                ending_index = try_index + index + 6;
            }
            None => {
                ending_index = contents.len() - 1;
            }
        }
        println!("sliced from prev_index: {}, to ending_index: {}", prev_index, ending_index);
        splits.push(&contents[prev_index..ending_index]);
        prev_index = ending_index;
    }
    splits
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
    let mut file_contents = Vec::<String>::new();
    let mut contents_split = Vec::<&str>::new();
    let before = time::Instant::now();

    for filename in index_filenames {
        println!("Reading {}...", filename);
        let file_content: String = fs::read_to_string(filename).unwrap();
        file_contents.push(file_content);
    }

    for raw_content in &file_contents {
        for contents in split_contents(&raw_content, "</doc>", 8) {
            contents_split.push(contents);
        }
    }

    println!("splits len: {}", contents_split.len());

    let duration = time::Instant::now() - before;
    println!("Reading Elapsed: {} ms", duration.as_millis());

    let num_threads = match matches.value_of("threads") {
        Some(t) => t.parse::<i32>().unwrap(),
        None => num_cpus::get() as i32
    };

    println!("Using {} threads to index", num_threads);
    let before_all = time::Instant::now();
    let analyzer: Analyzer = Analyzer::new_english();

    /*
    let (tx_doc, rx_doc): (crossbeam_channel::Sender<Vec<Document>>, crossbeam_channel::Receiver<Vec<Document>>) = crossbeam_channel::unbounded();
    let (tx_index, rx_index) = crossbeam_channel::unbounded();
    for _ in 0..num_threads {
        pool.spawn(|| index_thread(&analyzer, rx_doc.clone(), tx_index.clone()));
    }
    println!("AFTER SCOPE!!!!!!!!");
    drop(tx_index);
    */
    let pool = rayon::ThreadPoolBuilder::new().num_threads((num_threads as usize) + contents_split.len() + 1).build().unwrap();
    let inverted_index: InvertedIndex = InvertedIndex::with_capacity(2_000_000);
    let mut documents = pool.scope(|s| {
        let analyzer = &analyzer;
        let (tx_doc, _) = spawn_index_tasks(num_threads, s, &inverted_index, analyzer);
        // Async parse documents and push to indexing threads
        let rx_alldocs = parse_documents(&contents_split, s, tx_doc);

        // Read off indexing threads and merge
        /*let mut rx_index_iter = rx_index.into_iter();
        let mut joined_index = rx_index_iter.next().unwrap();
        println!("Starting merging...");
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
        }*/

        let mut all_docs_iter = rx_alldocs.into_iter();
        let mut documents: DocumentIndex = all_docs_iter.next().unwrap();
        for docs in all_docs_iter {
            documents.extend(docs);
        }
        documents
    });

    // Set up threads
    /*let inverted_index = crossbeam::thread::scope(|s| {
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

        // Parse documents and push to indexing threads
        for contents in file_contents.iter() {
            parse_documents(contents, &mut documents, &tx_doc);
        }
        drop(tx_doc);

        // Read off indexing threads and merge
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
    */

    let duration_all = time::Instant::now() - before_all;
    println!("Parsing and indexing elapsed: {} ms, Index size: {}, Num documents indexed: {}", duration_all.as_millis(), inverted_index.len(), documents.len());

    println!("Sorting...");
    let before = time::Instant::now();
    documents.sort();
    let duration = time::Instant::now() - before;
    println!("Sorting elapsed: {} ms", duration.as_millis());

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