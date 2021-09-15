use crate::indexers::*;
use std::hash::BuildHasherDefault;
use hashers::fx_hash::FxHasher;
use std::sync::atomic;
use core::ops::Range;
use rayon::prelude::*;
//use flexbuffers;
use bincode;
//use rmp_serde;
use std::time::{self};

pub type InvertedIndex = HashMapInvertedIndex;
pub type DocumentIndex = Vec<DocumentRaw>;

fn index_docs_index_only(full_contents: &str, documents: &[DocumentRaw], analyzer: &Analyzer) -> InvertedIndex {
    let mut inverted_index: InvertedIndex = InvertedIndex::with_capacity_and_hasher(500_000, BuildHasherDefault::<FxHasher>::default());
    
    for d in documents {
        //println!("text: {:?}, {}", d.text, &full_contents[d.text.clone()]);
        let text = &full_contents[d.text.clone()];
        //println!("analyzing {}", text);
        for token in analyzer.analyze(text) {
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

pub struct RayonIndexer { 
    index: InvertedIndex, 
    documents: DocumentIndex,
    full_contents: BoxedBytes,
    analyzer: Analyzer,
    cur_id: atomic::AtomicI32
}

impl RayonIndexer {
    pub fn new() -> Self {
        RayonIndexer { 
            index: InvertedIndex::with_capacity_and_hasher(2_000_000, BuildHasherDefault::<FxHasher>::default()), 
            documents: DocumentIndex::new(), 
            analyzer: Analyzer::new_english(),
            full_contents: Box::new(String::new()),
            cur_id: atomic::AtomicI32::new(0)
        }
    }
    fn parse_documents_vec(&self, file_contents: &ContentsSplit) -> DocumentIndex {
        let base_offset = file_contents.base_offset;
        let mut cur_doc = DocumentRaw::default();
        let mut cur_tag: &str = "";
        let mut docs: Vec<DocumentRaw> = Vec::with_capacity(500_000);
        //println!("len contents: {}", file_contents.len());
        for token in xmlparser::Tokenizer::from_fragment(file_contents.data, 0..file_contents.data.len()) {
            //println!("token: {:?}", token);
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
                            //println!("RANGE: {}\n{:?}\n{}\n{}\n", base_offset, absolute_range.clone(), 
                            //    &file_contents.data[text.range()], text.as_str());
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
                            cur_doc.id = self.cur_id.fetch_add(1, atomic::Ordering::SeqCst);
                            docs.push(cur_doc);
                            cur_doc = DocumentRaw::default();                        
                        }
                    }
                },
    
                Ok(_) => {},
                Err(e) => {println!("Error! {}, contents_start: {}", e, &file_contents.data[0..1024])}
            }
        }
        docs
    }
}

impl DocumentIndexer for RayonIndexer {
    fn build_from_file_contents(&mut self, file_contents: String) {
        let mut contents_split: Vec<ContentsSplit> = Vec::new();
        let num_threads = num_cpus::get();
        for contents in split_contents(&file_contents, "</doc>", num_threads) {
            contents_split.push(contents);
        }
        self.documents = contents_split.par_iter().map(|x| self.parse_documents_vec(x)).flatten().collect();
        self.documents.sort();
        self.index = self.documents.as_slice()
            .par_chunks(std::cmp::max(self.documents.len() / num_threads, 1))
            .map(|d| index_docs_index_only(&file_contents, d, &self.analyzer))
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
        self.full_contents = Box::new(file_contents);
    }
    fn build_from_serialized(&mut self, serialized_data: SerializedIndex) {
        let before = time::Instant::now();
        //let r = flexbuffers::Reader::get_root((*serialized_data.inverted_index).as_ref()).unwrap();
        //self.index = HashMapInvertedIndex::deserialize(r).unwrap();
        self.index = bincode::deserialize((*serialized_data.inverted_index).as_ref()).unwrap();
        //self.index = rmp_serde::from_read_ref((*serialized_data.inverted_index).as_ref()).unwrap();
        let after = time::Instant::now(); let total = after - before;
        println!("Index deserialize elapsed: {}", total.as_millis());
        let before = time::Instant::now();
        //let r = flexbuffers::Reader::get_root((*serialized_data.documents).as_ref()).unwrap();
        //self.documents = DocumentIndex::deserialize(r).unwrap();
        self.documents = bincode::deserialize((*serialized_data.documents).as_ref()).unwrap();
        //self.documents = rmp_serde::from_read_ref((*serialized_data.documents).as_ref()).unwrap();
        let after = time::Instant::now(); let total = after - before;
        println!("Documents deserialize elapsed: {}", total.as_millis());

        self.full_contents = serialized_data.file_contents;
    }
    fn get_serialized_inverted_index(&self) -> Vec<u8> {
        //let mut s = flexbuffers::FlexbufferSerializer::new();
        //self.index.serialize(&mut s).unwrap();
        //s.take_buffer()
        bincode::serialize(&self.index).unwrap()
        //rmp_serde::to_vec(&self.index).unwrap()
    }
    fn get_serialized_documents(&self) -> Vec<u8> {
        //let mut s = flexbuffers::FlexbufferSerializer::new();
        //self.documents.serialize(&mut s).unwrap();
        //s.take_buffer()
        bincode::serialize(&self.documents).unwrap()
        //rmp_serde::to_vec(&self.documents).unwrap()
    }

    fn search(&self, all_terms: Vec<&str>) -> Vec<SearchResults> {
        let mut results: Vec<SearchResults> = Vec::new();
        for search_term in all_terms {
            for term in self.analyzer.analyze(search_term) {
                if let Some(ids) = self.index.get(&term) {
                    let mut matched_docs: Vec<Document> = Vec::new();
                    for id in ids {
                        matched_docs.push(self.documents[*id as usize].to_document(self.full_contents.as_ref()));
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