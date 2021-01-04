mod rayon_indexer;
mod threadpool_indexer;
use std::hash::BuildHasherDefault;
use hashers::fx_hash::FxHasher;
use std::collections::HashMap;
use std::collections::HashSet;
use std::path::Path;
use std::fs::File;
use std::io;
use memmap;
use std::ops::Range;
use std::cmp;

pub use rayon_indexer::RayonIndexer;
pub use threadpool_indexer::ThreadPoolIndexer;

type HashMapInvertedIndex = HashMap<String, HashSet<i32, BuildHasherDefault<FxHasher>>, BuildHasherDefault<FxHasher>>;

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
pub struct Document {
    pub title: String,
    pub url: String,
    pub text: String,
    pub id: i32
}

impl<'a> PartialEq for Document {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for Document {}

impl PartialOrd for Document {
    fn partial_cmp(&self, other: &Document) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Document {
    fn cmp(&self, other: &Document) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

#[derive(Clone)]
pub struct DocumentRaw {
    pub title: Range<usize>,
    pub url: Range<usize>,
    pub text: Range<usize>,
    pub id: i32
}

impl DocumentRaw {
    fn to_document(&self, full_document: &(dyn AsRef<[u8]> + Sync)) -> Document {
        Document {
            title: String::from(std::str::from_utf8(&(*full_document).as_ref()[self.title.clone()]).unwrap()),
            url: String::from(std::str::from_utf8(&(*full_document).as_ref()[self.url.clone()]).unwrap()),
            text: String::from(std::str::from_utf8(&(*full_document).as_ref()[self.text.clone()]).unwrap()),
            id: self.id
        }
    }
}

impl PartialEq for DocumentRaw {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

impl Eq for DocumentRaw {}

impl PartialOrd for DocumentRaw {
    fn partial_cmp(&self, other: &DocumentRaw) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DocumentRaw {
    fn cmp(&self, other: &DocumentRaw) -> cmp::Ordering {
        self.id.cmp(&other.id)
    }
}

impl Default for DocumentRaw {
    fn default() -> Self {
        DocumentRaw {
            title: Range{start: 0, end: 0},
            url: Range{start: 0, end: 0},
            text: Range{start: 0, end: 0},
            id: 0
        }
    }
}

type BoxedBytes = Box<dyn AsRef<[u8]> + Sync>;

pub struct SearchResults {
    pub term: String,
    pub matches: Vec<Document>
}

pub struct SerializedIndex {
    inverted_index: BoxedBytes,
    documents: BoxedBytes,
    file_contents: BoxedBytes
}

// Adapted from
// https://github.com/tantivy-search/tantivy/blob/067ba3dff057da003e9d6722385867b6a506813f/src/directory/mmap_directory.rs#L37
fn open_mmap(full_path: &Path) -> Result<memmap::Mmap, io::Error> {
    let file = File::open(full_path)?;
    let meta_data = file.metadata()?;
    if meta_data.len() == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "empty file"));
    }
    unsafe {
        memmap::Mmap::map(&file)
    }
}

impl SerializedIndex {
    pub fn new(file_to_index_path: &str) -> Result<SerializedIndex, io::Error> {
        let base_path = Path::new(file_to_index_path);
        let inverted_index_path = base_path.with_extension("idx");
        let doc_index_path = base_path.with_extension("dcm");

        let file_content = open_mmap(base_path)?;
        let inverted_index = open_mmap(inverted_index_path.as_path())?;
        let doc_index = open_mmap(doc_index_path.as_path())?;

        Ok(SerializedIndex {
            inverted_index: Box::new(inverted_index),
            documents: Box::new(doc_index),
            file_contents: Box::new(file_content)
        })
    }
}

pub trait DocumentIndexer {
    fn build_from_file_contents(&mut self, file_contents: String);

    #[allow(unused_variables)]
    fn build_from_serialized(&mut self, serialized_data: SerializedIndex) {
        panic!("Not implemented");
    }
    fn get_index(&self) -> Vec<u8> {
        panic!("Not implemented");
    }
    fn search(&self, all_terms: Vec<&str>) -> Vec<SearchResults>;
    fn num_tokens(&self) -> usize;
    fn num_documents(&self) -> usize;
}

fn get_next_codepoint_idx(string: &str, try_index: usize) -> usize {
    let raw_bytes = string.as_bytes();
    let mut try_index = try_index;
    while try_index < raw_bytes.len() { 
        match std::str::from_utf8(&raw_bytes[try_index..try_index + std::mem::size_of::<char>()]) {
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

struct ContentsSplit<'a> {
    base_offset: usize,
    data: &'a str
}

fn split_contents<'a>(contents: &'a str, split_on_tag: &str, num_chunks: usize) -> Vec<ContentsSplit<'a>> {
    assert!(num_chunks > 0);
    if num_chunks <= 1 {
        return vec![ ContentsSplit{ base_offset: 0, data: contents } ];
    }
    let chars_per_split = contents.len() / num_chunks;
    let mut prev_index = 0;
    let mut splits: Vec<ContentsSplit<'a>> = Vec::new();
    for _ in 0..num_chunks {
        let try_index = get_next_codepoint_idx(contents, prev_index + chars_per_split);
        if try_index >= contents.len() {
            splits.push(ContentsSplit{ base_offset: prev_index, data: &contents[prev_index..]});
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
        splits.push(ContentsSplit{ base_offset: prev_index, data: &contents[prev_index..ending_index] });
        prev_index = ending_index;
    }
    splits
}