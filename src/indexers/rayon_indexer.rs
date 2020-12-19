use crate::indexers::*;
use std::hash::BuildHasherDefault;
use hashers::fx_hash::FxHasher;
use std::sync::atomic;
use core::ops::Range;
use rayon::prelude::*;

pub type InvertedIndex = HashMap<String, HashSet<i32, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>;
pub type DocumentIndex<'a> = Vec<Document<'a>>;

fn index_docs_index_only<'a>(documents: &[Document<'a>], analyzer: &Analyzer) -> InvertedIndex {
    let mut inverted_index: InvertedIndex = InvertedIndex::with_capacity_and_hasher(500_000, BuildHasherDefault::<FxHasher>::default());
    
    for d in documents {
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

    inverted_index
}

pub struct RayonIndexer<'a> { 
    index: InvertedIndex, 
    documents: DocumentIndex<'a>,
    analyzer: Analyzer,
    cur_id: atomic::AtomicI32
}

impl<'a> RayonIndexer<'a> {
    pub fn new() -> Self {
        RayonIndexer { 
            index: InvertedIndex::with_capacity_and_hasher(2_000_000, BuildHasherDefault::<FxHasher>::default()), 
            documents: DocumentIndex::new(), 
            analyzer: Analyzer::new_english(),
            cur_id: atomic::AtomicI32::new(0)
        }
    }

    pub fn parse_documents_vec(&self, file_contents: &'a str) -> Vec<Document<'a>> {
        let mut cur_doc = Document::default();
        let mut cur_tag: &str = "";
        let mut docs: Vec<Document> = Vec::with_capacity(500_000);
        //println!("len contents: {}", file_contents.len());
        for token in xmlparser::Tokenizer::from_fragment(file_contents, Range{start: 0, end: file_contents.len()}) {
            //println!("token: {:?}", token);
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
                            cur_doc.id = self.cur_id.fetch_add(1, atomic::Ordering::SeqCst);
                            docs.push(cur_doc);
                            cur_doc = Document::default();                        
                        }
                    }
                },
    
                Ok(_) => {},
                Err(e) => {println!("Error! {}, contents_start: {}", e, &file_contents[0..1024])}
            }
        }
        docs
    }
}

impl<'a> DocumentIndexer<'a> for RayonIndexer<'a> {

    fn build_index(&mut self, file_contents: &Vec<&'a str>) {
        let mut contents_split: Vec<&str> = Vec::new();
        let num_threads = num_cpus::get();
        for raw_content in file_contents {
            for contents in split_contents(&raw_content, "</doc>", num_threads) {
                contents_split.push(contents);
            }
        }
        self.documents = contents_split.par_iter().map(|x| self.parse_documents_vec(x)).flatten().collect();
        self.index = self.documents.as_slice()
            .par_chunks(std::cmp::max(self.documents.len() / num_threads, 1))
            .map(|d| index_docs_index_only(d, &self.analyzer))
            .reduce(
                || InvertedIndex::with_hasher(BuildHasherDefault::<FxHasher>::default()),
                |mut a, b| {
                    for (thread_k, thread_set) in b {
                        match a.get_mut(&thread_k) {
                            Some(joined_set) => {
                                joined_set.extend(thread_set);
                            },
                            None => {
                                a.insert(thread_k, thread_set);
                            }                 
                        }
                    }
                    a
                }
            );
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