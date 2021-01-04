use crate::indexers::*;

use std::sync::atomic;
use core::ops::Range;
use crossbeam;
use crossbeam::crossbeam_channel;
use dashmap;

pub type DashMapInvertedIndex = dashmap::DashMap<String, dashmap::DashSet<i32>>;
pub type DocumentIndex = Vec<DocumentRaw>;

type DocumentSender = crossbeam_channel::Sender<Vec<DocumentRaw>>;
type DocumentReceiver = crossbeam_channel::Receiver<Vec<DocumentRaw>>;
type IndexSender = crossbeam_channel::Sender<HashMapInvertedIndex>;
type IndexReceiver = crossbeam_channel::Receiver<HashMapInvertedIndex>;
type AllDocSender = crossbeam_channel::Sender<DocumentIndex>;
type AllDocReceiver = crossbeam_channel::Receiver<DocumentIndex>;

enum IndexType {
    SingleThread(HashMapInvertedIndex),
    MultiThread(DashMapInvertedIndex)
}

pub struct ThreadPoolIndexer {
    index: IndexType, 
    documents: DocumentIndex,
    analyzer: Analyzer,
    cur_id: atomic::AtomicI32,
    pool: rayon::ThreadPool,
    parse_threads: usize,
    index_threads: usize,
    full_contents: BoxedBytes
}

fn parse_task(contents: &ContentsSplit, tx_doc: DocumentSender, tx_alldocs: AllDocSender, cur_id: &atomic::AtomicI32) {
    let base_offset = contents.base_offset;
    let mut cur_doc = DocumentRaw::default();
    let mut cur_tag: &str = "";
    let chunk_size = 100;
    let mut chunk: Vec<DocumentRaw> = Vec::with_capacity(chunk_size);
    let mut all_docs: DocumentIndex = Vec::with_capacity(2_000_000);

    for token in xmlparser::Tokenizer::from_fragment(contents.data, Range{start: 0, end: contents.data.len()}) {
        match token {
            Ok(xmlparser::Token::ElementStart{local, ..}) => {
                cur_tag = local.as_str();
            },
            Ok(xmlparser::Token::Text{text}) => {
                let range_start = text.range().start;
                let range_end = text.range().end;
                let absolute_range = Range{start: base_offset+range_start, end: base_offset+range_end};
                match cur_tag {
                    "title" => cur_doc.title = absolute_range,
                    "abstract" => {
                        cur_doc.text = absolute_range
                    },
                    "url" => cur_doc.url = absolute_range,
                    _ => {}
                }
            },
            Ok(xmlparser::Token::ElementEnd{end, ..}) => {
                if let xmlparser::ElementEnd::Close(_, n) = end {
                    cur_tag = "";
                    if n.as_str() == "doc" {
                        cur_doc.id = cur_id.fetch_add(1, atomic::Ordering::SeqCst);
                        chunk.push(cur_doc.clone());
                        all_docs.push(cur_doc);
                    
                        if chunk.len() == chunk_size {
                            tx_doc.send(chunk).unwrap();
                            chunk = Vec::with_capacity(chunk_size);
                        }
                        cur_doc = DocumentRaw::default();
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

fn parse_documents<'b, 'a: 'b>(file_contents: Vec<ContentsSplit<'a>>, cur_id: &'b atomic::AtomicI32, scope: &rayon::Scope<'b>, tx_doc: DocumentSender) -> AllDocReceiver {
    let (tx_alldocs, rx_alldocs): (AllDocSender, AllDocReceiver) = crossbeam_channel::unbounded();
    for contents in file_contents {
        let tx_doc = tx_doc.clone();
        let tx_alldocs = tx_alldocs.clone();
        scope.spawn(move |_| {
            parse_task(&contents, tx_doc, tx_alldocs, cur_id)
        });    
    }
    rx_alldocs
}

fn index_task(rx_doc: DocumentReceiver, tx_index: IndexSender, analyzer: &Analyzer, full_contents: &str) {
    let mut inverted_index: HashMapInvertedIndex = HashMapInvertedIndex::with_capacity_and_hasher(500_000, BuildHasherDefault::<FxHasher>::default());
    for chunk in rx_doc {
        for d in chunk {
            for token in analyzer.analyze(&full_contents[d.text.clone()]) {
                match inverted_index.get_mut(&token) {
                    Some(set) => {
                        set.insert(d.id as i32);
                    }, 
                    None => {
                        let mut set = HashSet::with_hasher(BuildHasherDefault::<FxHasher>::default());
                        set.insert(d.id as i32);
                        inverted_index.insert(token, set);
                    }
                }
            }
        }
    }
    tx_index.send(inverted_index).unwrap();
}

fn dashmap_index_task(rx_doc: DocumentReceiver, inverted_index: &DashMapInvertedIndex, analyzer: &Analyzer, full_contents: &str) {
    for chunk in rx_doc {
        for d in chunk {
            for token in analyzer.analyze(&full_contents[d.text.clone()]) {
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
}

fn spawn_index_tasks<'a>(num_threads: usize, scope: &rayon::Scope<'a>, analyzer: &'a Analyzer, full_contents: &'a str) -> (DocumentSender, IndexReceiver) {
    let (tx_doc, rx_doc): (DocumentSender, DocumentReceiver) = crossbeam_channel::unbounded();
    let (tx_index, rx_index) = crossbeam_channel::unbounded();
    for _ in 0..num_threads {
        let rx_doc = rx_doc.clone();
        let tx_index = tx_index.clone();
        scope.spawn(move |_| {
            index_task(rx_doc, tx_index, analyzer, full_contents)
        });
    }
    (tx_doc, rx_index)
}

fn spawn_dashmap_index_tasks<'a>(num_threads: usize, inverted_index: &'a DashMapInvertedIndex, scope: &rayon::Scope<'a>, analyzer: &'a Analyzer, full_contents: &'a str) -> DocumentSender {
    let (tx_doc, rx_doc): (DocumentSender, DocumentReceiver) = crossbeam_channel::unbounded();
    for _ in 0..num_threads {
        let rx_doc = rx_doc.clone();
        scope.spawn(move |_| {
            dashmap_index_task(rx_doc, inverted_index, analyzer, full_contents);
        });
    }

    tx_doc
}

impl ThreadPoolIndexer {
    pub fn new_hashmap(parse_threads: usize, index_threads: usize) -> Self {
        ThreadPoolIndexer { 
            index: IndexType::SingleThread(HashMapInvertedIndex::with_hasher(BuildHasherDefault::<FxHasher>::default())), 
            documents: DocumentIndex::new(), 
            analyzer: Analyzer::new_english(),
            cur_id: atomic::AtomicI32::new(0),
            pool: rayon::ThreadPoolBuilder::new().num_threads(parse_threads + index_threads + 1).build().unwrap(),
            parse_threads: parse_threads,
            index_threads: index_threads,
            full_contents: Box::new(String::new()),
        }
    }
    
    pub fn new_dashmap(parse_threads: usize, index_threads: usize) -> Self {
        ThreadPoolIndexer { 
            index: IndexType::MultiThread(DashMapInvertedIndex::new()), 
            documents: DocumentIndex::new(), 
            analyzer: Analyzer::new_english(),
            cur_id: atomic::AtomicI32::new(0),
            pool: rayon::ThreadPoolBuilder::new().num_threads(parse_threads + index_threads + 1).build().unwrap(),
            parse_threads: parse_threads,
            index_threads: index_threads,
            full_contents: Box::new(String::new()),
        }
    }

    fn build_hashmap(&self, contents_split: Vec<ContentsSplit>, full_contents: &str) -> (HashMapInvertedIndex, DocumentIndex) {
        let pool = &self.pool;
        let analyzer = &self.analyzer;
        let cur_id = &self.cur_id;
        let (inverted_index, documents) = pool.scope(|s| {
            let (tx_doc, rx_index) = spawn_index_tasks(self.index_threads, s, &analyzer, full_contents);

            // Async parse documents and push to indexing threads
            let rx_alldocs = parse_documents(contents_split, &cur_id, s, tx_doc);
    
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
    
            let mut all_docs_iter = rx_alldocs.into_iter();
            let mut documents: DocumentIndex = all_docs_iter.next().unwrap();
            for docs in all_docs_iter {
                documents.extend(docs);
            }
            (joined_index, documents)
        });

        (inverted_index, documents)
    }

    fn build_dashmap(&self, contents_split: Vec<ContentsSplit>, full_contents: &str) -> (DashMapInvertedIndex, DocumentIndex) {
        let pool = &self.pool;
        let analyzer = &self.analyzer;
        let cur_id = &self.cur_id;
        let inverted_index = DashMapInvertedIndex::with_capacity(2_000_000);
        let documents = pool.scope(|s| {
            let tx_doc = spawn_dashmap_index_tasks(self.index_threads, &inverted_index, s, &analyzer, full_contents);

            // Async parse documents and push to indexing threads
            let rx_alldocs = parse_documents(contents_split, &cur_id, s, tx_doc);
    
            let mut all_docs_iter = rx_alldocs.into_iter();
            let mut documents: DocumentIndex = all_docs_iter.next().unwrap();
            for docs in all_docs_iter {
                documents.extend(docs);
            }
            documents
        });

        (inverted_index, documents)
    }
}

macro_rules! search {
    ($s:expr, $idx:expr, $all_terms:expr ,$results:expr) => {
        for search_term in $all_terms {
            for term in $s.analyzer.analyze(search_term) {
                if let Some(ids) = $idx.get(&term) {
                    let mut matched_docs: Vec<Document> = Vec::new();
                    for id in ids.iter() {
                        matched_docs.push($s.documents[*id as usize].to_document($s.full_contents.as_ref()));
                    }
                    $results.push(SearchResults{term: term, matches: matched_docs});
                }
            }
        }
    };
}

impl DocumentIndexer for ThreadPoolIndexer {
    fn build_from_file_contents(&mut self, file_contents: String) {
        //self.file_contents = file_contents;
        let mut contents_split: Vec<ContentsSplit> = Vec::new();
        println!("NUM CPUS: {}", num_cpus::get());
        for contents in split_contents(&file_contents, "</doc>", self.parse_threads) {
            contents_split.push(contents);
        }

        if let IndexType::SingleThread(_) = self.index {
            let (index, documents) = self.build_hashmap(contents_split, &file_contents);
            self.index = IndexType::SingleThread(index);
            self.documents = documents;
        } else {
            let (index, documents) = self.build_dashmap(contents_split, &file_contents);
            self.index = IndexType::MultiThread(index);
            self.documents = documents;
        }
        self.documents.sort();
        self.full_contents = Box::new(file_contents);
    }
    
    fn search(&self, all_terms: Vec<&str>) -> Vec<SearchResults> {
        let mut results: Vec<SearchResults> = Vec::new();
        match &self.index {
            IndexType::SingleThread(idx) => search!(self, idx, all_terms, results),
            IndexType::MultiThread(idx) => search!(self, idx, all_terms, results)
        }
        results
    }

    fn num_tokens(&self) -> usize {
        match &self.index {
            IndexType::SingleThread(idx) => idx.len(),
            IndexType::MultiThread(idx) => idx.len()
        }
    }
    fn num_documents(&self) -> usize {
        self.documents.len()
    }
}