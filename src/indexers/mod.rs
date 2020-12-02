mod rayon_indexer;
mod threadpool_indexer;
pub use rayon_indexer::RayonIndexer;
pub use threadpool_indexer::ThreadPoolIndexer;

use std::collections::HashMap;
use std::collections::HashSet;

struct Analyzer {
    stopwords: HashSet<&'static str>,
    stemmer: rust_stemmers::Stemmer,
}

impl Analyzer {
    fn new_english() -> Analyzer {
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

#[derive(Default, Clone)]
pub struct Document<'a> {
    pub title: &'a str,
    pub url: &'a str,
    pub text: &'a str,
    pub id: i32
}

pub struct SearchResults<'a> {
    pub term: String,
    pub matches: Vec<&'a Document<'a>>,
}

pub trait DocumentIndexer<'a> {
    fn build_index(&mut self, file_contents: Vec<&'a str>);
    fn search(&self, all_terms: Vec<&str>) -> Vec<SearchResults>;
    fn num_tokens(&self) -> usize;
    fn num_documents(&self) -> usize;
}

fn get_next_codepoint_idx(string: &str, try_index: usize) -> usize {
    let raw_bytes = string.as_bytes();
    let mut try_index = try_index;
    while try_index < raw_bytes.len() { 
        match std::str::from_utf8(&raw_bytes[try_index..try_index+std::mem::size_of::<char>()]) {
            Ok(_) => {
                return try_index;
            },
            Err(_) => {
                try_index += 1;
            }
        }
    }
    try_index
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
        let try_index = get_next_codepoint_idx(contents, prev_index + chars_per_split);
        if try_index >= contents.len() {
            splits.push(&contents[prev_index..]);
            //println!("sliced from prev_index: {}, to ending_index: {}", prev_index, contents.len());
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
        //println!("sliced from prev_index: {}, to ending_index: {}", prev_index, ending_index);
        splits.push(&contents[prev_index..ending_index]);
        prev_index = ending_index;
    }
    splits
}