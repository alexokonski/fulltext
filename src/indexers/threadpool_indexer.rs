use crate::indexers::*;
use std::hash::BuildHasherDefault;
use hashers::fx_hash::FxHasher;
use std::sync::atomic;
use core::ops::Range;
use crossbeam;
use crossbeam::crossbeam_channel;

pub type InvertedIndex = HashMap<String, HashSet<i32, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>;
pub type DocumentIndex<'a> = Vec<Document<'a>>;

type DocumentSender<'a> = crossbeam_channel::Sender<Vec<Document<'a>>>;
type DocumentReceiver<'a> = crossbeam_channel::Receiver<Vec<Document<'a>>>;
type IndexSender = crossbeam_channel::Sender<InvertedIndex>;
type IndexReceiver = crossbeam_channel::Receiver<InvertedIndex>;
type AllDocSender<'a> = crossbeam_channel::Sender<DocumentIndex<'a>>;
type AllDocReceiver<'a> = crossbeam_channel::Receiver<DocumentIndex<'a>>;

pub struct ThreadPoolIndexer<'a> { 
    file_contents: Vec<&'a str>,
    index: InvertedIndex, 
    documents: DocumentIndex<'a>,
    analyzer: Analyzer,
    cur_id: atomic::AtomicI32,
    pool: rayon::ThreadPool
}

fn parse_task<'a>(contents: &'a str, tx_doc: DocumentSender<'a>, tx_alldocs: AllDocSender<'a>, cur_id: &atomic::AtomicI32) {
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
                        cur_doc.id = cur_id.fetch_add(1, atomic::Ordering::SeqCst);
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

fn parse_documents<'b, 'a: 'b>(file_contents: Vec<&'a str>, cur_id: &'b atomic::AtomicI32, scope: &rayon::Scope<'b>, tx_doc: DocumentSender<'a>) -> AllDocReceiver<'a> {
    let (tx_alldocs, rx_alldocs): (AllDocSender, AllDocReceiver) = crossbeam_channel::unbounded();
    for contents in file_contents {
        let tx_doc = tx_doc.clone();
        let tx_alldocs = tx_alldocs.clone();
        scope.spawn(move |_| {
            parse_task(contents, tx_doc, tx_alldocs, cur_id)
        });    
    }
    rx_alldocs
}

fn index_task<'b>(rx_doc: DocumentReceiver, tx_index: IndexSender, analyzer: &'b Analyzer) {
    let mut inverted_index: InvertedIndex = InvertedIndex::with_capacity_and_hasher(500_000, BuildHasherDefault::<FxHasher>::default());
    for chunk in rx_doc {
        for d in chunk {
            for token in analyzer.analyze(&d.text) {
                match inverted_index.get_mut(&token) {
                    Some(set) => {
                        set.insert(d.id as i32);
                    }, 
                    None => {
                        let mut set = HashSet::with_capacity_and_hasher(5, BuildHasherDefault::<FxHasher>::default());
                        set.insert(d.id as i32);
                        inverted_index.insert(token, set);
                    }
                }
            }
        }
    }
    tx_index.send(inverted_index).unwrap();
}

fn spawn_index_tasks<'b, 'a: 'b>(num_threads: usize, scope: &rayon::Scope<'b>, analyzer: &'b Analyzer) -> (DocumentSender<'a>, IndexReceiver) {
    let (tx_doc, rx_doc): (DocumentSender<'a>, DocumentReceiver<'a>) = crossbeam_channel::unbounded();
    let (tx_index, rx_index) = crossbeam_channel::unbounded();
    for _ in 0..num_threads {
        let rx_doc = rx_doc.clone();
        let tx_index = tx_index.clone();
        scope.spawn(move |_| {
            index_task(rx_doc, tx_index, analyzer)
        });
    }
    (tx_doc, rx_index)
}

impl<'a> From<Vec<&'a str>> for ThreadPoolIndexer<'a> {
    fn from(file_contents: Vec<&'a str>) -> Self {
        let default_threads = num_cpus::get() / 2;
        let mut indexer = ThreadPoolIndexer::new(default_threads, default_threads);
        indexer.build_index(file_contents);
        indexer
    }
}

impl<'a> ThreadPoolIndexer<'a> {
    pub fn new(parse_threads: usize, index_threads: usize) -> Self {
        ThreadPoolIndexer { 
            file_contents: Vec::new(),
            index: InvertedIndex::with_capacity_and_hasher(2_000_000, BuildHasherDefault::<FxHasher>::default()), 
            documents: DocumentIndex::new(), 
            analyzer: Analyzer::new_english(),
            cur_id: atomic::AtomicI32::new(0),
            pool: rayon::ThreadPoolBuilder::new().num_threads(parse_threads + index_threads + 1).build().unwrap()
        }
    }
}

impl<'a> DocumentIndexer<'a> for ThreadPoolIndexer<'a> {  
    fn build_index(&mut self, file_contents: Vec<&'a str>) {
        self.file_contents = file_contents;
        let mut contents_split: Vec<&str> = Vec::new();
        //let num_threads = num_cpus::get();
        println!("NUM CPUS: {}", num_cpus::get());
        for raw_content in &self.file_contents {
            for contents in split_contents(&raw_content, "</doc>", 6) {
                contents_split.push(contents);
            }
        }

        let pool = &self.pool;
        let analyzer = &self.analyzer;
        let cur_id = &self.cur_id;
        let (inverted_index, documents) = pool.scope(|s| {
            let (tx_doc, rx_index) = spawn_index_tasks(10, s, &analyzer);
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
        self.index = inverted_index;
        self.documents = documents;
    }
    
    fn search(&self, all_terms: Vec<&str>) -> Vec<SearchResults> {
        let mut results: Vec<SearchResults> = Vec::new();
        for search_term in all_terms {
            for term in self.analyzer.analyze(search_term) {
                if let Some(ids) = self.index.get(&term) {
                    let mut matched_docs: Vec<&Document> = Vec::new();
                    for id in ids {
                        matched_docs.push(&self.documents[*id as usize]);
                    }
                    results.push(SearchResults{term: term, matches: matched_docs});
                }
            }
        }
    
        results
    }
    fn num_tokens(&self) -> usize {
        self.index.len()
    }
    fn num_documents(&self) -> usize {
        self.documents.len()
    }
}