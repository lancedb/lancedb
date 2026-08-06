// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

// the first 32-bits of a FSST compressed file is the FSST magic number
const FSST_MAGIC: u64 = 0x46535354 << 32; // "FSST"
// when the code is FSST_ESC, the next byte should be interpreted as is
const FSST_ESC: u8 = 255;
// when building symbol table, we have a maximum of 512 symbols, so we can use 9 bits to represent the code
const FSST_CODE_BITS: u16 = 9;
// when building symbol table, we use the first 256 codes to represent the index itself, for example, code 0 represents byte 0
const FSST_CODE_BASE: u16 = 256;

// code 512, which we can never reach(maximum code is 511)
const FSST_CODE_MAX: u16 = 1 << FSST_CODE_BITS;
// all code bits set
const FSST_CODE_MASK: u16 = FSST_CODE_MAX - 1;
// we construct FSST symbol tables using a random sample of about 16KB (1<<14)
const FSST_SAMPLETARGET: usize = 1 << 14;
const FSST_SAMPLEMAXSZ: usize = 2 * FSST_SAMPLETARGET;

// if the input size is less than 32 KB, we mark the file header and copy the input to the output as is
pub const FSST_LEAST_INPUT_SIZE: usize = 32 * 1024;

// if the max length of the input strings are less than `FSST_LEAST_INPUT_MAX_LENGTH`, we shouldn't use FSST.
pub const FSST_LEAST_INPUT_MAX_LENGTH: u64 = 5;

// we only use the lower 32 bits in icl, so we can use 1 << 32 to represent a free slot in the hash table
const FSST_ICL_FREE: u64 = 1 << 32;
// in the icl field of a symbol, the symbol length is stored in 4 bits starting from the 28th bit
const CODE_LEN_SHIFT_IN_ICL: u64 = 28;
// in the icl field of a symbol, the symbol code is stored in the 12 bits starting from the 16th bit
const CODE_SHIFT_IN_ICL: u64 = 16;

const CODE_LEN_SHIFT_IN_CODE: u64 = 12;

const FSST_HASH_TAB_SIZE: usize = 1024;
const FSST_HASH_PRIME: u64 = 2971215073;
const FSST_SHIFT: usize = 15;
#[inline]
fn fsst_hash(w: u64) -> u64 {
    w.wrapping_mul(FSST_HASH_PRIME) ^ ((w.wrapping_mul(FSST_HASH_PRIME)) >> FSST_SHIFT)
}

const MAX_SYMBOL_LENGTH: usize = 8;

pub const FSST_SYMBOL_TABLE_SIZE: usize = 8 + 256 * 8 + 256; // 8 bytes for the header, 256 symbols(8 bytes each), 256 bytes for lens

use arrow_array::OffsetSizeTrait;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::io;
use std::marker::PhantomData;
use std::ptr;

#[inline]
fn fsst_unaligned_load_unchecked(v: *const u8) -> u64 {
    // SAFETY: the caller must guarantee that `v` points to at least 8 readable bytes. All callers
    // uphold this: `compress_bulk` loads from a 520-byte stack buffer at an offset < 511 (leaving
    // >= 8 bytes), `build_symbol_table` guards the load with `word.len() > 7 && curr < word.len() - 7`,
    // `find_longest_symbol_from_char_slice` copies into a stack `[u8; 8]` before loading, and
    // `FsstDecoder::init` reads symbols from a `symbol_table` buffer already validated to be
    // exactly `FSST_SYMBOL_TABLE_SIZE` bytes.
    unsafe { ptr::read_unaligned(v as *const u64) }
}

#[derive(Default, Copy, Clone, PartialEq, Eq)]
struct Symbol {
    // the byte sequence that this symbol stands for
    val: u64,

    // icl = u64 ignoredBits:16,code:12,length:4,unused:32 -- but we avoid exposing this bit-field notation
    // use a single u64 to be sure "code" is accessed with one load and can be compared with one comparison
    icl: u64,
}

use std::fmt;

impl fmt::Display for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.val.to_ne_bytes();
        for i in 0..self.symbol_len() {
            write!(f, "{}", bytes[i as usize] as char)?;
        }
        write!(f, "\t")?;
        write!(
            f,
            "ignoredBits: {}, code: {}, length: {}",
            self.ignored_bits(),
            self.code(),
            self.symbol_len()
        )?;
        Ok(())
    }
}

impl fmt::Debug for Symbol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.val.to_ne_bytes();
        for i in 0..self.symbol_len() {
            write!(f, "{}", bytes[i as usize] as char)?;
        }
        write!(f, "\t")?;
        write!(
            f,
            "ignoredBits: {}, code: {}, length: {}",
            self.ignored_bits(),
            self.code(),
            self.symbol_len()
        )?;
        Ok(())
    }
}

impl Symbol {
    fn new() -> Self {
        Self {
            val: 0,
            icl: FSST_ICL_FREE,
        }
    }

    fn from_char(c: u8, code: u16) -> Self {
        Self {
            val: c as u64,
            // in a symbol which represents a single character, 56 bits(7 bytes) are ignored, code length is 1
            icl: (1 << CODE_LEN_SHIFT_IN_ICL) | ((code as u64) << CODE_SHIFT_IN_ICL) | 56,
        }
    }

    fn set_code_len(&mut self, code: u16, len: u32) {
        self.icl = ((len as u64) << CODE_LEN_SHIFT_IN_ICL)
            | ((code as u64) << CODE_SHIFT_IN_ICL)
            | ((8u64.saturating_sub(len as u64)) * 8);
    }

    #[inline]
    fn symbol_len(&self) -> u32 {
        (self.icl >> CODE_LEN_SHIFT_IN_ICL) as u32
    }

    #[inline]
    fn code(&self) -> u16 {
        ((self.icl >> CODE_SHIFT_IN_ICL) & FSST_CODE_MASK as u64) as u16
    }

    // ignoredBits is (8-length)*8, which is the amount of high bits to zero in the input word before comparing with the hashtable key
    // it could of course be computed from len during lookup, but storing it precomputed in some loose bits is faster
    #[inline]
    fn ignored_bits(&self) -> u32 {
        (self.icl & u16::MAX as u64) as u32
    }

    #[inline]
    fn first(&self) -> u8 {
        assert!(self.symbol_len() >= 1);
        (0xFF & self.val) as u8
    }

    #[inline]
    fn first2(&self) -> u16 {
        assert!(self.symbol_len() >= 2);
        (0xFFFF & self.val) as u16
    }

    #[inline]
    fn hash(&self) -> u64 {
        let v = 0xFFFFFF & self.val;
        fsst_hash(v)
    }

    // right is the substring follows left
    // for example, in "hello",
    // "llo" is the substring that follows "he"
    fn concat(left: Self, right: Self) -> Self {
        let mut s = Self::new();
        let mut length = left.symbol_len() + right.symbol_len();
        if length > MAX_SYMBOL_LENGTH as u32 {
            length = MAX_SYMBOL_LENGTH as u32;
        }
        s.set_code_len(FSST_CODE_MASK, length);
        s.val = (right.val << (8 * left.symbol_len())) | left.val;
        s
    }
}

// Symbol that can be put in a queue, ordered on gain
#[derive(Clone)]
struct QSymbol {
    symbol: Symbol,
    // the gain field is only used in the symbol queue that sorts symbols on gain
    gain: u32,
}

impl PartialEq for QSymbol {
    fn eq(&self, other: &Self) -> bool {
        self.symbol.val == other.symbol.val && self.symbol.icl == other.symbol.icl
    }
}

impl Ord for QSymbol {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.gain
            .cmp(&other.gain)
            .then_with(|| other.symbol.val.cmp(&self.symbol.val))
    }
}

impl PartialOrd for QSymbol {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for QSymbol {}

use std::hash::{Hash, Hasher};

impl Hash for QSymbol {
    // this hash algorithm follows the C++ implementation of the FSST in the paper
    fn hash<H: Hasher>(&self, state: &mut H) {
        let mut k = self.symbol.val;
        const M: u64 = 0xc6a4a7935bd1e995;
        const R: u32 = 47;
        let mut h: u64 = 0x8445d61a4e774912 ^ (8u64.wrapping_mul(M));
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
        h ^= h >> R;
        h = h.wrapping_mul(M);
        h ^= h >> R;
        h.hash(state);
    }
}

#[derive(Clone)]
struct SymbolTable {
    short_codes: [u16; 65536],
    byte_codes: [u16; 256],
    symbols: [Symbol; FSST_CODE_MAX as usize],
    hash_tab: [Symbol; FSST_HASH_TAB_SIZE],
    n_symbols: u16,
    terminator: u16,
    // in a finalized symbol table, symbols are arranged by their symbol length,
    // in the order of 2, 3, 4, 5, 6, 7, 8, 1, codes < suffix_lim are 2 bytes codes that don't have a longer suffix
    suffix_lim: u16,
    len_histo: [u8; FSST_CODE_BITS as usize],
}

impl std::fmt::Display for SymbolTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "A FSST SymbolTable after finalize():")?;
        writeln!(f, "n_symbols: {}", self.n_symbols)?;
        for i in 0_usize..self.n_symbols as usize {
            writeln!(f, "symbols[{}]: {}", i, self.symbols[i])?;
        }
        writeln!(f, "suffix_lim: {}", self.suffix_lim)?;
        for i in 0..FSST_CODE_BITS {
            writeln!(f, "len_histo[{}]: {}", i, self.len_histo[i as usize])?;
        }
        Ok(())
    }
}

impl std::fmt::Debug for SymbolTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "A FSST SymbolTable before finalize():")?;
        writeln!(f, "n_symbols: {}", self.n_symbols)?;
        for i in FSST_CODE_BASE as usize..FSST_CODE_BASE as usize + self.n_symbols as usize {
            writeln!(f, "symbols[{}]: {}", i, self.symbols[i])?;
        }
        writeln!(f, "suffix_lim: {}", self.suffix_lim)?;
        for i in 0..FSST_CODE_BITS {
            writeln!(f, "len_histo[{}]: {}\n", i, self.len_histo[i as usize])?;
        }
        Ok(())
    }
}

impl SymbolTable {
    fn new() -> Self {
        let mut symbols = [Symbol::new(); FSST_CODE_MAX as usize];
        for (i, symbol) in symbols.iter_mut().enumerate().take(256) {
            *symbol = Symbol::from_char(i as u8, i as u16);
        }
        let unused = Symbol::from_char(0, FSST_CODE_MASK);
        for i in 256..FSST_CODE_MAX {
            symbols[i as usize] = unused;
        }
        let s = Symbol::new();
        let hash_tab = [s; FSST_HASH_TAB_SIZE];
        let mut byte_codes = [0; 256];
        for (i, byte_code) in byte_codes.iter_mut().enumerate() {
            *byte_code = i as u16;
        }
        let mut short_codes = [FSST_CODE_MASK; 65536];
        for i in 0..=65535_u16 {
            short_codes[i as usize] = i & 0xFF;
        }
        Self {
            short_codes,
            byte_codes,
            symbols,
            hash_tab,
            n_symbols: 0,
            terminator: 256,
            suffix_lim: FSST_CODE_MAX,
            len_histo: [0; FSST_CODE_BITS as usize],
        }
    }

    fn clear(&mut self) {
        for i in 0..256 {
            self.symbols[i] = Symbol::from_char(i as u8, i as u16);
        }
        let unused = Symbol::from_char(0, FSST_CODE_MASK);
        for i in 256..FSST_CODE_MAX {
            self.symbols[i as usize] = unused;
        }
        for i in 0..256 {
            self.byte_codes[i] = i as u16;
        }
        for i in 0..=65535_u16 {
            self.short_codes[i as usize] = i & 0xFF;
        }
        let s = Symbol::new();
        for i in 0..FSST_HASH_TAB_SIZE {
            self.hash_tab[i] = s;
        }
        for i in 0..FSST_CODE_BITS as usize {
            self.len_histo[i] = 0;
        }
        self.n_symbols = 0;
    }

    fn hash_insert(&mut self, s: Symbol) -> bool {
        let idx = (s.hash() & (FSST_HASH_TAB_SIZE as u64 - 1)) as usize;
        let taken = self.hash_tab[idx].icl < FSST_ICL_FREE;
        if taken {
            return false; // collision in hash table
        }
        self.hash_tab[idx].icl = s.icl;
        self.hash_tab[idx].val = s.val & (u64::MAX >> (s.ignored_bits()));
        true
    }

    fn add(&mut self, mut s: Symbol) -> bool {
        assert!(FSST_CODE_BASE + self.n_symbols < FSST_CODE_MAX);
        let len = s.symbol_len();
        s.set_code_len(FSST_CODE_BASE + self.n_symbols, len);
        if len == 1 {
            self.byte_codes[s.first() as usize] = FSST_CODE_BASE + self.n_symbols;
        } else if len == 2 {
            self.short_codes[s.first2() as usize] = FSST_CODE_BASE + self.n_symbols;
        } else if !self.hash_insert(s) {
            return false;
        }
        self.symbols[(FSST_CODE_BASE + self.n_symbols) as usize] = s;
        self.n_symbols += 1;
        self.len_histo[(len - 1) as usize] += 1;
        true
    }

    fn find_longest_symbol_from_char_slice(&self, input: &[u8]) -> u16 {
        let len = if input.len() >= MAX_SYMBOL_LENGTH {
            MAX_SYMBOL_LENGTH
        } else {
            input.len()
        };
        if len < 2 {
            return self.byte_codes[input[0] as usize] & FSST_CODE_MASK;
        }
        if len == 2 {
            let short_code = ((input[1] as usize) << 8) | input[0] as usize;
            if self.short_codes[short_code] >= FSST_CODE_BASE {
                return self.short_codes[short_code] & FSST_CODE_MASK;
            } else {
                return self.byte_codes[input[0] as usize] & FSST_CODE_MASK;
            }
        }
        let mut input_in_1_word = [0; 8];
        input_in_1_word[..len].copy_from_slice(&input[..len]);
        let input_in_u64 = fsst_unaligned_load_unchecked(input_in_1_word.as_ptr());
        let hash_idx = fsst_hash(input_in_u64) as usize & (FSST_HASH_TAB_SIZE - 1);
        let s_in_hash_tab = self.hash_tab[hash_idx];
        if s_in_hash_tab.icl < FSST_ICL_FREE
            && s_in_hash_tab.val == (input_in_u64 & (u64::MAX >> s_in_hash_tab.ignored_bits()))
        {
            return s_in_hash_tab.code();
        }
        self.byte_codes[input[0] as usize] & FSST_CODE_MASK
    }

    // rationale for finalize:
    // - during symbol table construction, we may create more than 256 codes, but bring it down to max 255 in the last makeTable()
    //   consequently we needed more than 8 bits during symbol table construction, but can simplify the codes to single bytes in finalize()
    //   (this feature is in fact lo longer used, but could still be exploited: symbol construction creates no more than 255 symbols in each pass)
    // - we not only reduce the amount of codes to <255, but also *reorder* the symbols and renumber their codes, for higher compression perf.
    //   we renumber codes so they are grouped by length, to allow optimized scalar string compression (byteLim and suffixLim optimizations).
    // - we make the use of byteCode[] no longer necessary by inserting single-byte codes in the free spots of shortCodes[]
    //   Using shortCodes[] only makes compression faster. When creating the symbolTable, however, using shortCodes[] for the single-byte
    //   symbols is slow, as each insert touches 256 positions in it. This optimization was added when optimizing symbolTable construction time.
    //
    // In all, we change the layout and coding, as follows..
    //
    // before finalize():
    // - The real symbols are symbols[256..256+nSymbols>. As we may have nSymbols > 255
    // - The first 256 codes are pseudo symbols (all escaped bytes)
    //
    // after finalize():
    // - table layout is symbols[0..nSymbols>, with nSymbols < 256.
    // - Real codes are [0,nSymbols>. 8-th bit not set.
    // - Escapes in shortCodes have the 8th bit set (value: 256+255=511). 255 because the code to be emitted is the escape byte 255
    // - symbols are grouped by length: 2,3,4,5,6,7,8, then 1 (single-byte codes last)
    // the two-byte codes are split in two sections:
    // - first section contains codes for symbols for which there is no longer symbol (no suffix). It allows an early-out during compression
    //
    // finally, shortCodes[] is modified to also encode all single-byte symbols (hence byteCodes[] is not required on a critical path anymore).
    fn finalize(&mut self) {
        assert!(self.n_symbols < FSST_CODE_BASE);
        let mut new_code: [u16; 256] = [0; 256];
        let mut rsum: [u8; 8] = [0; 8];
        let byte_lim = self.n_symbols - self.len_histo[0] as u16;

        rsum[0] = byte_lim as u8; // 1-byte codes are highest
        for i in 1..7 {
            rsum[i + 1] = rsum[i] + self.len_histo[i];
        }

        let mut suffix_lim = 0;
        let mut j = rsum[2];
        for i in 0..self.n_symbols {
            let mut s1 = self.symbols[(FSST_CODE_BASE + i) as usize];
            let len = s1.symbol_len();
            let opt = if len == 2 { self.n_symbols } else { 0 };
            if opt != 0 {
                let mut has_suffix = false;
                let first2 = s1.first2();
                for k in 0..opt {
                    let s2 = self.symbols[(FSST_CODE_BASE + k) as usize];
                    if k != i && s2.symbol_len() > 2 && first2 == s2.first2() {
                        has_suffix = true;
                    }
                }
                new_code[i as usize] = if has_suffix {
                    suffix_lim += 1;
                    suffix_lim - 1
                } else {
                    j -= 1;
                    j as u16
                };
            } else {
                new_code[i as usize] = rsum[(len - 1) as usize] as u16;
                rsum[(len - 1) as usize] += 1;
            }
            s1.set_code_len(new_code[i as usize], len);
            self.symbols[new_code[i as usize] as usize] = s1;
        }

        for i in 0..256 {
            if (self.byte_codes[i] & FSST_CODE_MASK) >= FSST_CODE_BASE {
                self.byte_codes[i] =
                    new_code[(self.byte_codes[i] & 0xFF) as usize] | (1 << CODE_LEN_SHIFT_IN_CODE);
            } else {
                self.byte_codes[i] = 511 | (1 << CODE_LEN_SHIFT_IN_CODE);
            }
        }

        for i in 0..65536 {
            if (self.short_codes[i] & FSST_CODE_MASK) > FSST_CODE_BASE {
                self.short_codes[i] =
                    new_code[(self.short_codes[i] & 0xFF) as usize] | (2 << CODE_LEN_SHIFT_IN_CODE);
            } else {
                self.short_codes[i] = self.byte_codes[i & 0xFF] | (1 << CODE_LEN_SHIFT_IN_CODE);
            }
        }

        for i in 0..FSST_HASH_TAB_SIZE {
            if self.hash_tab[i].icl < FSST_ICL_FREE {
                self.hash_tab[i] =
                    self.symbols[new_code[(self.hash_tab[i].code() & 0xFF) as usize] as usize];
            }
        }
        self.suffix_lim = suffix_lim;
    }
}

#[derive(Clone)]
struct Counters {
    count1: Vec<u16>,
    count2: Vec<Vec<u16>>,
}

impl Counters {
    fn new() -> Self {
        Self {
            count1: vec![0; FSST_CODE_MAX as usize],
            count2: vec![vec![0; FSST_CODE_MAX as usize]; FSST_CODE_MAX as usize],
        }
    }

    #[inline]
    fn count1_set(&mut self, pos1: usize, val: u16) {
        self.count1[pos1] = val;
    }

    #[inline]
    fn count1_inc(&mut self, pos1: u16) {
        self.count1[pos1 as usize] = self.count1[pos1 as usize].saturating_add(1);
    }

    #[inline]
    fn count2_inc(&mut self, pos1: usize, pos2: usize) {
        self.count2[pos1][pos2] = self.count2[pos1][pos2].saturating_add(1);
    }

    #[inline]
    fn count1_get(&self, pos1: usize) -> u16 {
        self.count1[pos1]
    }

    #[inline]
    fn count2_get(&self, pos1: usize, pos2: usize) -> u16 {
        self.count2[pos1][pos2]
    }
}

#[inline]
fn is_escape_code(pos: u16) -> bool {
    pos < FSST_CODE_BASE
}

// make_sample selects strings randoms from the input, and returns a set of strings of size around FSST_SAMPLETARGET
fn make_sample<T: OffsetSizeTrait>(in_buf: &[u8], offsets: &[T]) -> (Vec<u8>, Vec<T>) {
    let total_size = in_buf.len();
    if total_size <= FSST_SAMPLETARGET {
        return (in_buf.to_vec(), offsets.to_vec());
    }
    let mut sample_buf = Vec::with_capacity(FSST_SAMPLEMAXSZ);
    let mut sample_offsets: Vec<T> = Vec::new();

    sample_offsets.push(T::from_usize(0).unwrap());
    let mut rng = StdRng::from_os_rng();
    while sample_buf.len() < FSST_SAMPLETARGET {
        let rand_num = rng.random_range(0..offsets.len()) % (offsets.len() - 1);
        sample_buf.extend_from_slice(
            &in_buf[offsets[rand_num].as_usize()..offsets[rand_num + 1].as_usize()],
        );
        sample_offsets.push(T::from_usize(sample_buf.len()).unwrap());
    }
    sample_offsets.push(T::from_usize(sample_buf.len()).unwrap());
    (sample_buf, sample_offsets)
}

// build_symbol_table constructs a symbol table from a sample of the input
fn build_symbol_table<T: OffsetSizeTrait>(
    sample_buf: Vec<u8>,
    sample_offsets: Vec<T>,
) -> io::Result<Box<SymbolTable>> {
    let mut st = SymbolTable::new();
    let mut best_table = SymbolTable::new();
    // worst case (everything exception), will be updated later
    let mut best_gain = T::zero() - T::from_usize(FSST_SAMPLEMAXSZ).unwrap();

    let mut byte_histo = [0; 256];
    for c in &sample_buf {
        byte_histo[*c as usize] += 1;
    }
    let mut curr_min_histo = FSST_SAMPLEMAXSZ;

    for (i, this_byte_histo) in byte_histo.iter().enumerate() {
        if *this_byte_histo < curr_min_histo {
            curr_min_histo = *this_byte_histo;
            st.terminator = i as u16;
        }
    }

    // Compress sample, and compute (pair-)frequencies
    let compress_count = |st: &mut SymbolTable, sample_frac: usize| -> (Box<Counters>, T) {
        let mut gain = T::from_usize(0).unwrap();
        let mut counters = Counters::new();

        for i in 1..sample_offsets.len() {
            if sample_offsets[i] == sample_offsets[i - 1] {
                continue;
            }
            let word = &sample_buf[sample_offsets[i - 1].as_usize()..sample_offsets[i].as_usize()];

            let mut curr = 0;
            let mut curr_code;
            let mut prev_code = st.find_longest_symbol_from_char_slice(&word[curr..]);
            curr += st.symbols[prev_code as usize].symbol_len() as usize;

            // Avoid arithmetic on Option<T>
            let symbol_len = st.symbols[prev_code as usize].symbol_len() as usize;
            let escape_cost = if is_escape_code(prev_code) { 1 } else { 0 };
            let gain_contribution = symbol_len.saturating_sub(1 + escape_cost);
            gain += T::from_usize(gain_contribution).unwrap();

            while curr < word.len() {
                counters.count1_inc(prev_code);
                let symbol_len;

                if st.symbols[prev_code as usize].symbol_len() != 1 {
                    counters.count1_inc(word[curr] as u16);
                }

                if word.len() > 7 && curr < word.len() - 7 {
                    let mut this_64_bit_word: u64 =
                        fsst_unaligned_load_unchecked(word[curr..].as_ptr());
                    let code = this_64_bit_word & 0xFFFFFF;
                    let idx = fsst_hash(code) as usize & (FSST_HASH_TAB_SIZE - 1);
                    let s: Symbol = st.hash_tab[idx];
                    let short_code =
                        st.short_codes[(this_64_bit_word & 0xFFFF) as usize] & FSST_CODE_MASK;
                    this_64_bit_word &= 0xFFFFFFFFFFFFFFFF >> s.icl as u8;
                    if (s.icl < FSST_ICL_FREE) & (s.val == this_64_bit_word) {
                        curr_code = s.code();
                        symbol_len = s.symbol_len();
                    } else if short_code >= FSST_CODE_BASE {
                        curr_code = short_code;
                        symbol_len = 2;
                    } else {
                        curr_code =
                            st.byte_codes[(this_64_bit_word & 0xFF) as usize] & FSST_CODE_MASK;
                        symbol_len = 1;
                    }
                } else {
                    curr_code = st.find_longest_symbol_from_char_slice(&word[curr..]);
                    symbol_len = st.symbols[curr_code as usize].symbol_len();
                }

                // Avoid arithmetic on Option<T>
                let symbol_len_usize = symbol_len as usize;
                let escape_cost = if is_escape_code(curr_code) { 1 } else { 0 };
                let gain_contribution = symbol_len_usize.saturating_sub(1 + escape_cost);
                gain += T::from_usize(gain_contribution).unwrap();

                // no need to count pairs in final round
                if sample_frac < 128 {
                    // consider the symbol that is the concatenation of the last two symbols
                    counters.count2_inc(prev_code as usize, curr_code as usize);
                    if symbol_len > 1 {
                        counters.count2_inc(prev_code as usize, word[curr] as usize);
                    }
                }
                curr += symbol_len as usize;
                prev_code = curr_code;
            }
            counters.count1_inc(prev_code);
        }
        (Box::new(counters), gain)
    };

    let make_table = |st: &mut SymbolTable, counters: &mut Counters, sample_frac: usize| {
        let mut candidates: HashSet<QSymbol> = HashSet::new();

        counters.count1_set(st.terminator as usize, u16::MAX);

        let add_or_inc = |cands: &mut HashSet<QSymbol>, s: Symbol, count: u64| {
            if count < (5 * sample_frac as u64) / 128 {
                return;
            }
            let mut q = QSymbol {
                symbol: s,
                gain: (count * s.symbol_len() as u64) as u32,
            };
            if let Some(old_q) = cands.get(&q) {
                q.gain += old_q.gain;
                cands.remove(&old_q.clone());
            }
            cands.insert(q);
        };

        // add candidate symbols based on counted frequencies
        for pos1 in 0..FSST_CODE_BASE as usize + st.n_symbols as usize {
            let cnt1 = counters.count1_get(pos1);
            if cnt1 == 0 {
                continue;
            }
            // heuristic: promoting single-byte symbols (*8) helps reduce exception rates and increases [de]compression speed
            let s1 = st.symbols[pos1];
            add_or_inc(
                &mut candidates,
                s1,
                if s1.symbol_len() == 1 { 8 } else { 1 } * cnt1 as u64,
            );
            if s1.first() == st.terminator as u8 {
                continue;
            }
            if sample_frac >= 128
                || s1.symbol_len() == MAX_SYMBOL_LENGTH as u32
                || s1.first() == st.terminator as u8
            {
                continue;
            }
            for pos2 in 0..FSST_CODE_BASE as usize + st.n_symbols as usize {
                let cnt2 = counters.count2_get(pos1, pos2);
                if cnt2 == 0 {
                    continue;
                }

                // create a new symbol
                let s2 = st.symbols[pos2];
                let s3 = Symbol::concat(s1, s2);
                // multi-byte symbols cannot contain the terminator byte
                if s2.first() != st.terminator as u8 {
                    add_or_inc(&mut candidates, s3, cnt2 as u64);
                }
            }
        }
        let mut pq: BinaryHeap<QSymbol> = BinaryHeap::new();
        for q in &candidates {
            pq.push(q.clone());
        }

        // Create new symbol map using best candidates
        st.clear();
        while st.n_symbols < 255 && !pq.is_empty() {
            let q = pq.pop().unwrap();
            st.add(q.symbol);
        }
    };

    for frac in [8, 38, 68, 98, 108, 128] {
        // we do 5 rounds (sampleFrac=8,38,68,98,128)
        let (mut this_counter, gain) = compress_count(&mut st, frac);
        if gain >= best_gain {
            // a new best solution
            best_gain = gain;
            best_table = st.clone();
        }
        make_table(&mut st, &mut this_counter, frac);
    }
    best_table.finalize(); // renumber codes for more efficient compression
    if best_table.n_symbols == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "Fsst failed to build symbol table, input len: {}, input_offsets len: {}",
                sample_buf.len(),
                sample_offsets.len()
            ),
        ));
    }
    Ok(Box::new(best_table))
}

fn compress_bulk<T: OffsetSizeTrait>(
    st: &SymbolTable,
    strs: &[u8],
    offsets: &[T],
    out: &mut Vec<u8>,
    out_offsets: &mut Vec<T>,
    out_pos: &mut usize,
    out_offsets_len: &mut usize,
) -> io::Result<()> {
    let mut out_curr = *out_pos;

    let mut compress = |buf: &[u8], in_end: usize, out_curr: &mut usize| {
        let mut in_curr = 0;
        while in_curr < in_end {
            let word = fsst_unaligned_load_unchecked(buf[in_curr..].as_ptr());
            let short_code = st.short_codes[(word & 0xFFFF) as usize];
            let word_first_3_byte = word & 0xFFFFFF;
            let idx = fsst_hash(word_first_3_byte) as usize & (FSST_HASH_TAB_SIZE - 1);
            let s = st.hash_tab[idx];
            out[*out_curr + 1] = word as u8; // speculatively write out escaped byte
            let code = if s.icl < FSST_ICL_FREE && s.val == (word & (u64::MAX >> (s.icl & 0xFFFF)))
            {
                (s.icl >> 16) as u16
            } else {
                short_code
            };
            out[*out_curr] = code as u8;
            in_curr += (code >> 12) as usize;
            *out_curr += 1 + ((code & 256) >> 8) as usize;
        }
    };

    out_offsets[0] = T::from_usize(*out_pos).unwrap();
    for i in 1..offsets.len() {
        let mut in_curr = offsets[i - 1].as_usize();
        let end_curr = offsets[i].as_usize();
        let mut buf: [u8; 520] = [0; 520]; // +8 sentinel is to avoid 8-byte unaligned-loads going beyond 511 out-of-bounds
        while in_curr < end_curr {
            let in_end = std::cmp::min(in_curr + 511, end_curr);
            {
                let this_len = in_end - in_curr;
                buf[..this_len].copy_from_slice(&strs[in_curr..in_end]);
                buf[this_len] = st.terminator as u8; // sentinel
            }
            compress(&buf, in_end - in_curr, &mut out_curr);
            in_curr = in_end;
        }
        out_offsets[i] = T::from_usize(out_curr).unwrap();
    }

    out.resize(out_curr, 0); // shrink to actual size
    out_offsets.resize(offsets.len(), T::from_usize(0).unwrap()); // shrink to actual size
    *out_pos = out_curr;
    *out_offsets_len = offsets.len();
    Ok(())
}

fn decompress_bulk<T: OffsetSizeTrait>(
    decoder: &FsstDecoder<T>,
    compressed_strs: &[u8],
    offsets: &[T],
    out: &mut Vec<u8>,
    out_offsets: &mut Vec<T>,
    out_pos: &mut usize,
    out_offsets_len: &mut usize,
) -> io::Result<()> {
    let symbols = decoder.symbols;
    let lens = decoder.lens;
    // SAFETY invariant shared by every `unsafe` block in this closure:
    // - `out` is sized to at least 8x `compressed_strs` (checked in `FsstDecoder::init`, which the
    //   sole public entry point always runs before reaching this function). Each code advances
    //   `out_curr` by `lens[code]`, which is 1..=8 for a well-formed symbol table, and each
    //   consumed input byte yields at most 8 output bytes, so `out_curr + 8 <= out.len()` at every
    //   8-byte write, including the final one. This is why we can `write_unaligned` a full 8-byte
    //   word per code and advance by only the length.
    //   NOTE: `lens` is loaded verbatim from the (untrusted) symbol table and is NOT re-validated
    //   to be <= 8 on decode, and offsets (below) are likewise trusted. A corrupted table or
    //   offset buffer can violate these bounds; callers must supply structures produced by
    //   `compress` (or otherwise trusted). Hardening the decoder against corrupt input is a
    //   separate concern, not addressed here.
    // - The only unchecked read is `read_unaligned::<u32>`, gated by `in_curr + 4 <= in_end`; the
    //   scalar paths use bounds-checked indexing. `in_end` is a caller-provided offset into
    //   `compressed_strs`; the read is sound only if `in_end <= compressed_strs.len()`, which is a
    //   trusted precondition (holds for encoder-produced offsets; not validated here).
    let mut decompress = |mut in_curr: usize, in_end: usize, out_curr: &mut usize| {
        // Do SIMD operation here by 4 bytes
        while in_curr + 4 <= in_end {
            let next_block;
            let mut code;
            let mut len;
            // SAFETY: the loop guard proves `in_curr + 4 <= in_end`. Per the closure-level
            // invariant, `in_end <= compressed_strs.len()` is a trusted precondition (not checked
            // here), so the 4-byte read is in bounds for well-formed input.
            unsafe {
                next_block =
                    ptr::read_unaligned(compressed_strs.as_ptr().add(in_curr) as *const u32);
            }
            let escape_mask = (next_block & 0x80808080u32)
                & ((((!next_block) & 0x7F7F7F7Fu32) + 0x7F7F7F7Fu32) ^ 0x80808080u32);
            if escape_mask == 0 {
                // 0th byte
                code = compressed_strs[in_curr] as usize;
                len = lens[code] as usize;
                unsafe {
                    let src = symbols[code];
                    ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                }
                in_curr += 1;
                *out_curr += len;

                // 1st byte
                code = compressed_strs[in_curr] as usize;
                len = lens[code] as usize;
                unsafe {
                    let src = symbols[code];
                    ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                }
                in_curr += 1;
                *out_curr += len;

                // 2nd byte
                code = compressed_strs[in_curr] as usize;
                len = lens[code] as usize;
                unsafe {
                    let src = symbols[code];
                    ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                }
                in_curr += 1;
                *out_curr += len;

                // 3rd byte
                code = compressed_strs[in_curr] as usize;
                len = lens[code] as usize;
                unsafe {
                    let src = symbols[code];
                    ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                }
                in_curr += 1;
                *out_curr += len;
            } else {
                let first_escape_pos = escape_mask.trailing_zeros() >> 3;
                if first_escape_pos == 3 {
                    // 0th byte
                    code = compressed_strs[in_curr] as usize;
                    len = lens[code] as usize;
                    unsafe {
                        let src = symbols[code];
                        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                    }
                    in_curr += 1;
                    *out_curr += len;

                    // 1st byte
                    code = compressed_strs[in_curr] as usize;
                    len = lens[code] as usize;
                    unsafe {
                        let src = symbols[code];
                        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                    }
                    in_curr += 1;
                    *out_curr += len;

                    // 2nd byte
                    code = compressed_strs[in_curr] as usize;
                    len = lens[code] as usize;
                    unsafe {
                        let src = symbols[code];
                        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                    }
                    in_curr += 1;
                    *out_curr += len;

                    // escape byte
                    in_curr += 2;
                    out[*out_curr] = compressed_strs[in_curr - 1];
                    *out_curr += 1;
                } else if first_escape_pos == 2 {
                    // 0th byte
                    code = compressed_strs[in_curr] as usize;
                    len = lens[code] as usize;
                    unsafe {
                        let src = symbols[code];
                        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                    }
                    in_curr += 1;
                    *out_curr += len;

                    // 1st byte
                    code = compressed_strs[in_curr] as usize;
                    len = lens[code] as usize;
                    unsafe {
                        let src = symbols[code];
                        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                    }
                    in_curr += 1;
                    *out_curr += len;

                    // escape byte
                    in_curr += 2;
                    out[*out_curr] = compressed_strs[in_curr - 1];
                    *out_curr += 1;
                } else if first_escape_pos == 1 {
                    // 0th byte
                    code = compressed_strs[in_curr] as usize;
                    len = lens[code] as usize;
                    unsafe {
                        let src = symbols[code];
                        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                    }
                    in_curr += 1;
                    *out_curr += len;

                    // escape byte
                    in_curr += 2;
                    out[*out_curr] = compressed_strs[in_curr - 1];
                    *out_curr += 1;
                } else {
                    // escape byte
                    in_curr += 2;
                    out[*out_curr] = compressed_strs[in_curr - 1];
                    *out_curr += 1;
                }
            }
        }

        // handle the remaining bytes
        if in_curr + 2 <= in_end {
            out[*out_curr] = compressed_strs[in_curr + 1];
            if compressed_strs[in_curr] != FSST_ESC {
                let code = compressed_strs[in_curr] as usize;
                unsafe {
                    let src = symbols[code];
                    ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                }
                in_curr += 1;
                *out_curr += lens[code] as usize;
                if compressed_strs[in_curr] != FSST_ESC {
                    let code = compressed_strs[in_curr] as usize;
                    unsafe {
                        let src = symbols[code];
                        ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
                    }
                    in_curr += 1;
                    *out_curr += lens[code] as usize;
                } else {
                    in_curr += 2;
                    out[*out_curr] = compressed_strs[in_curr - 1];
                    *out_curr += 1;
                }
            } else {
                in_curr += 2;
                *out_curr += 1;
            }
        }

        if in_curr < in_end {
            // last code cannot be an escape code
            let code = compressed_strs[in_curr] as usize;
            // SAFETY: see the closure-level invariant. This is the final write and has no
            // subsequent write to cover its slack, so it is the tightest case: `out_curr` is at
            // most 8*(consumed_input_bytes - 1), and the 8-byte store lands within `out.len()`
            // precisely because the caller sized `out` to 8x the input.
            unsafe {
                let src = symbols[code];
                ptr::write_unaligned(out.as_mut_ptr().add(*out_curr) as *mut u64, src);
            }
            *out_curr += lens[code] as usize;
        }
    };

    let mut out_curr = *out_pos;
    out_offsets[0] = T::from_usize(*out_pos).unwrap();
    for i in 1..offsets.len() {
        let in_curr = offsets[i - 1].as_usize();
        let in_end = offsets[i].as_usize();
        decompress(in_curr, in_end, &mut out_curr);
        out_offsets[i] = T::from_usize(out_curr).unwrap();
    }
    out.resize(out_curr, 0);
    out_offsets.resize(offsets.len(), T::from_usize(0).unwrap());
    *out_pos = out_curr;
    *out_offsets_len = offsets.len();
    Ok(())
}

struct FsstEncoder<T: OffsetSizeTrait> {
    symbol_table: Box<SymbolTable>,
    // when in_buf is less than FSST_LEAST_INPUT_SIZE, we simply copy the input to the output
    encoder_switch: bool,
    _phantom: PhantomData<T>,
}

impl<T: OffsetSizeTrait> FsstEncoder<T> {
    fn new() -> Self {
        Self {
            symbol_table: Box::new(SymbolTable::new()),
            encoder_switch: false,
            _phantom: PhantomData,
        }
    }

    fn init(
        &mut self,
        in_buf: &[u8],
        in_offsets_buf: &[T],
        out_buf: &[u8],
        out_offsets_buf: &[T],
        symbol_table: &[u8],
    ) -> io::Result<()> {
        // should we have a symbol_table MAGIC footer here?
        if symbol_table.len() != FSST_SYMBOL_TABLE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "the symbol table buffer for FSST encoder must have size {}",
                    FSST_SYMBOL_TABLE_SIZE
                ),
            ));
        }

        if in_buf.len() < FSST_LEAST_INPUT_SIZE {
            return Ok(());
        }

        // currently, we make sure the compression output buffer has at least the same size as the input buffer,
        if in_buf.len() > out_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "output buffer ({}) too small for FSST encoder (need at least {})",
                    out_buf.len(),
                    in_buf.len()
                ),
            ));
        }
        if in_offsets_buf.len() > out_offsets_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "output offsets buffer ({}) too small for FSST encoder (need at least {})",
                    out_offsets_buf.len(),
                    in_offsets_buf.len()
                ),
            ));
        }

        self.encoder_switch = true;
        let (sample, sample_offsets) = make_sample(in_buf, in_offsets_buf);
        let st = build_symbol_table(sample, sample_offsets)?;
        self.symbol_table = st;
        Ok(())
    }

    fn export(&self, symbol_table_buf: &mut [u8]) -> io::Result<()> {
        let st = &self.symbol_table;

        let st_info: u64 = FSST_MAGIC
            | ((self.encoder_switch as u64) << 24)
            | (((st.suffix_lim & 255) as u64) << 16)
            | (((st.terminator & 255) as u64) << 8)
            | ((st.n_symbols & 255) as u64);

        let st_info_bytes = st_info.to_ne_bytes();
        let mut pos = 0;
        symbol_table_buf[pos..pos + st_info_bytes.len()].copy_from_slice(&st_info_bytes);

        pos += st_info_bytes.len();

        for i in 0..st.n_symbols as usize {
            let s = st.symbols[i];
            let s_bytes = s.val.to_ne_bytes();
            symbol_table_buf[pos..pos + s_bytes.len()].copy_from_slice(&s_bytes);
            pos += s_bytes.len();
        }
        for i in 0..st.n_symbols as usize {
            let this_len = st.symbols[i].symbol_len();
            symbol_table_buf[pos] = this_len as u8;
            pos += 1;
        }
        Ok(())
    }

    fn compress(
        &mut self,
        in_buf: &[u8],
        in_offsets_buf: &[T],
        out_buf: &mut Vec<u8>,
        out_offsets_buf: &mut Vec<T>,
        symbol_table_buf: &mut [u8],
    ) -> io::Result<()> {
        self.init(
            in_buf,
            in_offsets_buf,
            out_buf,
            out_offsets_buf,
            symbol_table_buf,
        )?;
        self.export(symbol_table_buf)?;

        // if the input buffer is less than FSST_LEAST_INPUT_SIZE, we simply copy the input to the output
        if !self.encoder_switch {
            out_buf.resize(in_buf.len(), 0);
            out_buf.copy_from_slice(in_buf);
            out_offsets_buf.resize(in_offsets_buf.len(), T::from_usize(0).unwrap());
            out_offsets_buf.copy_from_slice(in_offsets_buf);
            return Ok(());
        }
        let mut out_pos = 0;
        let mut out_offsets_len = 0;
        compress_bulk(
            &self.symbol_table,
            in_buf,
            in_offsets_buf,
            out_buf,
            out_offsets_buf,
            &mut out_pos,
            &mut out_offsets_len,
        )?;
        Ok(())
    }
}

const FSST_CORRUPT: u64 = 32774747032022883; // 7-byte number in little endian containing "corrupt"
struct FsstDecoder<T: OffsetSizeTrait> {
    lens: [u8; 256],
    symbols: [u64; 256],
    decoder_switch_on: bool,
    _phantom: PhantomData<T>,
}

impl<T: OffsetSizeTrait> FsstDecoder<T> {
    fn new() -> Self {
        Self {
            lens: [0; 256],
            symbols: [FSST_CORRUPT; 256],
            decoder_switch_on: false,
            _phantom: PhantomData,
        }
    }

    fn init(
        &mut self,
        symbol_table: &[u8],
        in_buf: &[u8],
        in_offsets_buf: &[T],
        out_buf: &[u8],
        out_offsets_buf: &[T],
    ) -> io::Result<()> {
        let st_info = u64::from_ne_bytes(symbol_table[..8].try_into().unwrap());
        if st_info & FSST_MAGIC != FSST_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "the input buffer is not a valid FSST compressed data",
            ));
        }

        if symbol_table.len() != FSST_SYMBOL_TABLE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "the symbol table buffer for FSST decoder must have size {}",
                    FSST_SYMBOL_TABLE_SIZE
                ),
            ));
        }

        self.decoder_switch_on = (st_info & (1 << 24)) != 0;
        // A single 1-byte code can decode to a symbol of up to MAX_SYMBOL_LENGTH (8) bytes, so the
        // decoded output can be up to 8x the input. `decompress_bulk` also relies on this bound: it
        // writes a full 8-byte word per code (advancing only by the symbol length), so the output
        // buffer must be large enough that even the final write stays in bounds. Require out_buf to
        // be at least 8x in_buf. `checked_mul` guards against `in_buf.len() * 8` wrapping on 32-bit
        // targets (an input >= 512 MiB would otherwise bypass the check); treat overflow as too
        // small.
        if self.decoder_switch_on
            && in_buf
                .len()
                .checked_mul(8)
                .is_none_or(|needed| needed > out_buf.len())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output buffer too small for FSST decoder",
            ));
        }

        // when decoder_switch_on is false, we make sure the out_buf is at least the same size of the in_buf,
        if !self.decoder_switch_on && in_buf.len() > out_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "output buffer too small for FSST decoder",
            ));
        }

        if in_offsets_buf.len() > out_offsets_buf.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "output offsets buffer ({}) too small for FSST decoder (need at least {})",
                    out_offsets_buf.len(),
                    in_offsets_buf.len()
                ),
            ));
        }
        let symbol_num = (st_info & 255) as u8;
        let mut pos = 8;
        for i in 0..symbol_num as usize {
            self.symbols[i] = fsst_unaligned_load_unchecked(symbol_table[pos..].as_ptr());
            pos += 8;
        }
        for i in 0..symbol_num as usize {
            self.lens[i] = symbol_table[pos];
            pos += 1;
        }
        Ok(())
    }

    fn decompress(
        &mut self,
        in_buf: &[u8],
        in_offsets_buf: &[T],
        out_buf: &mut Vec<u8>,
        out_offsets_buf: &mut Vec<T>,
    ) -> io::Result<()> {
        if !self.decoder_switch_on {
            out_buf.resize(in_buf.len(), 0);
            out_buf.copy_from_slice(in_buf);
            out_offsets_buf.resize(in_offsets_buf.len(), T::from_usize(0).unwrap());
            out_offsets_buf.copy_from_slice(in_offsets_buf);
            return Ok(());
        }
        let mut out_pos = 0;
        let mut out_offsets_len = 0;
        decompress_bulk(
            self,
            in_buf,
            in_offsets_buf,
            out_buf,
            out_offsets_buf,
            &mut out_pos,
            &mut out_offsets_len,
        )?;
        Ok(())
    }
}

/// This is the public API for the FSST compression, when the in_buf is less than FSST_LEAST_INPUT_SIZE, we put the FSST_MAGIC header and then copy the input to the output
/// we check to make sure the out_buf's size is at least the same as the in_buf's size, otherwise Err is returned, this is actually
/// risky as in some randomly generated data, the output size can be larger than the input size.
/// the out_offsets_buf should be at least the same size as the in_offsets_buf, otherwise Err is returned
/// the symbol_table is used to store the symbol table created by `compression`, it's size should be FSST_SYMBOL_TABLE_SIZE
/// after compression, the first 64 bits of the output buffer is the fsst header:
/// from most significant bit to least significant bit:
/// FSST_MAGIC| encoder_switch |    suffix_lim | terminator | n_symbols
/// | 32-bits |         8 bits |        8 bits |     8 bits | 8 bits
/// then followed by the compressed data
///
pub fn compress<T: OffsetSizeTrait>(
    symbol_table: &mut [u8],
    in_buf: &[u8],
    in_offsets_buf: &[T],
    out_buf: &mut Vec<u8>,
    out_offsets_buf: &mut Vec<T>,
) -> io::Result<()> {
    FsstEncoder::new().compress(
        in_buf,
        in_offsets_buf,
        out_buf,
        out_offsets_buf,
        symbol_table,
    )?;
    Ok(())
}
// This is the public API for the FSST decompression, when the first 32 bits of in_buf is not the FSST_MAGIC, we know the input is not a
// valid FSST compressed data and return an error
// the following 32 bits after FSST_MAGIC contains information about FSST encoding, such as decoder_switch_on, suffix_lim, terminator, n_symbols
// when the decoder_switch_on is off in the in_buf header, `decompress` first make sure the out_buf is at least the same size as the in_buf, then simply copy the
// input data to the output
// when the decoder_switch_on is on, `decompress` first make sure the out_buf is at least 8 times the size of the in_buf, then start decoding the
// data using the symbol table. The 8x bound is required for correctness: a 1-byte code can expand to
// an 8-byte symbol, and the decode loop writes a full 8-byte word per code, so a smaller buffer can
// be written out of bounds.
// the out_offsets_buf should be at least the same size as the in_offsets_buf, otherwise an error is returned
// the symbol_table is the same symbol table created by `compression`
pub fn decompress<T: OffsetSizeTrait>(
    symbol_table: &[u8],
    in_buf: &[u8],
    in_offsets_buf: &[T],
    out_buf: &mut Vec<u8>,
    out_offsets_buf: &mut Vec<T>,
) -> io::Result<()> {
    let mut decoder = FsstDecoder::new();
    decoder.init(
        symbol_table,
        in_buf,
        in_offsets_buf,
        out_buf,
        out_offsets_buf,
    )?;
    decoder.decompress(in_buf, in_offsets_buf, out_buf, out_offsets_buf)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::fsst::*;
    use arrow_array::StringArray;

    const TEST_PARAGRAPH: &str = "ACT I. Scene I.
    Elsinore. A platform before the Castle.

    Enter two Sentinels-[first,] Francisco, [who paces up and down
    at his post; then] Bernardo, [who approaches him].

        Ber. Who's there.?
        Fran. Nay, answer me. Stand and unfold yourself.
        Ber. Long live the King!
        Fran. Bernardo?
        Ber. He.
        Fran. You come most carefully upon your hour.
        Ber. 'Tis now struck twelve. Get thee to bed, Francisco.
        Fran. For this relief much thanks. 'Tis bitter cold,
        And I am sick at heart.
        Ber. Have you had quiet guard?
        Fran. Not a mouse stirring.
        Ber. Well, good night.
        If you do meet Horatio and Marcellus,
        The rivals of my watch, bid them make haste.
        Enter Horatio and Marcellus.

        Fran. I think I hear them. Stand, ho! Who is there?
        Hor. Friends to this ground.
        Mar. And liegemen to the Dane.
        Fran. Give you good night.
        Mar. O, farewell, honest soldier.
            Who hath reliev'd you?
        Fran. Bernardo hath my place.
            Give you good night.                                   Exit.
        Mar. Holla, Bernardo!
        Ber. Say-
            What, is Horatio there ?
        Hor. A piece of him.
        Ber. Welcome, Horatio. Welcome, good Marcellus.
        Mar. What, has this thing appear'd again to-night?
        Ber. I have seen nothing.
        Mar. Horatio says 'tis but our fantasy,
            And will not let belief take hold of him
            Touching this dreaded sight, twice seen of us.
            Therefore I have entreated him along,
            With us to watch the minutes of this night,
            That, if again this apparition come,
            He may approve our eyes and speak to it.
        Hor. Tush, tush, 'twill not appear.
        Ber. Sit down awhile,
            And let us once again assail your ears,
            That are so fortified against our story,
            What we two nights have seen.
        Hor. Well, sit we down,
            And let us hear Bernardo speak of this.
        Ber. Last night of all,
            When yond same star that's westward from the pole
            Had made his course t' illume that part of heaven
            Where now it burns, Marcellus and myself,
            The bell then beating one-

                                Enter Ghost.

        Mar. Peace! break thee off! Look where it comes again!
        Ber. In the same figure, like the King that's dead.
        Mar. Thou art a scholar; speak to it, Horatio.
        Ber. Looks it not like the King? Mark it, Horatio.
        Hor. Most like. It harrows me with fear and wonder.
        Ber. It would be spoke to.
        Mar. Question it, Horatio.
        Hor. What art thou that usurp'st this time of night
            Together with that fair and warlike form
            In which the majesty of buried Denmark
            Did sometimes march? By heaven I charge thee speak!
        Mar. It is offended.
        Ber. See, it stalks away!
        Hor. Stay! Speak, speak! I charge thee speak!
                                                            Exit Ghost.
        Mar. 'Tis gone and will not answer.
        Ber. How now, Horatio? You tremble and look pale.
            Is not this something more than fantasy?
            What think you on't?
        Hor. Before my God, I might not this believe
            Without the sensible and true avouch
            Of mine own eyes.
        Mar. Is it not like the King?
        Hor. As thou art to thyself.
            Such was the very armour he had on
            When he th' ambitious Norway combated.
            So frown'd he once when, in an angry parle,
            He smote the sledded Polacks on the ice.
            'Tis strange.
        Mar. Thus twice before, and jump at this dead hour,
            With martial stalk hath he gone by our watch.
        Hor. In what particular thought to work I know not;
            But, in the gross and scope of my opinion,
            This bodes some strange eruption to our state.
        Mar. Good now, sit down, and tell me he that knows,
            Why this same strict and most observant watch
            So nightly toils the subject of the land,
            And why such daily cast of brazen cannon
            And foreign mart for implements of war;
            Why such impress of shipwrights, whose sore task
            Does not divide the Sunday from the week.
            What might be toward, that this sweaty haste
            Doth make the night joint-labourer with the day?
            Who is't that can inform me?";

    const TEST_PARAGRAPH2: &str = "Towards the end of November, during a thaw, at nine o’clock one morning, a train on the Warsaw and Petersburg railway was approaching the latter city at full speed.
The morning was so damp and misty that it was only with great difficulty that the day succeeded in breaking;
and it was impossible to distinguish anything more than a few yards away from the carriage windows.
Some of the passengers by this particular train were returning from abroad; but the third-class carriages were the best filled, chiefly with insignificant persons of various occupations and degrees,
picked up at the different stations nearer town.
All of them seemed weary, and most of them had sleepy eyes and a shivering expression, while their complexions generally appeared to have taken on the colour of the fog outside.
When day dawned, two passengers in one of the third-class carriages found themselves opposite each other. Both were young fellows, both were rather poorly dressed, both had remarkable faces,
and both were evidently anxious to start a conversation.
If they had but known why, at this particular moment, they were both remarkable persons, they would undoubtedly have wondered at the strange chance which had set them down opposite to one another in a third-class carriage of the Warsaw Railway Company.
One of them was a young fellow of about twenty-seven, not tall, with black curling hair, and small, grey, fiery eyes. His nose was broad and flat, and he had high cheek bones; his thin lips were constantly compressed into an impudent,
ironical—it might almost be called a malicious—smile;
but his forehead was high and well formed, and atoned for a good deal of the ugliness of the lower part of his face.
A special feature of this physiognomy was its death-like pallor, which gave to the whole man an indescribably emaciated appearance in spite of his hard look,
and at the same time a sort of passionate and suffering expression which did not harmonize with his impudent,
sarcastic smile and keen, self-satisfied bearing.
He wore a large fur—or rather astrachan—overcoat, which had kept him warm all night, while his neighbour had been obliged to bear the full severity of a Russian November night entirely unprepared.
His wide sleeveless mantle with a large cape to it—the sort of cloak one sees upon travellers during the winter months in Switzerland or North Italy—was by no means adapted to the long cold journey through Russia, from Eydkuhnen to St. Petersburg.
The wearer of this cloak was a young fellow, also of about twenty-six or twenty-seven years of age, slightly above the middle height, very fair, with a thin, pointed and very light coloured beard;
his eyes were large and blue, and had an intent look about them, yet that heavy expression which some people affirm to be a peculiarity as well as evidence, of an epileptic subject.
His face was decidedly a pleasant one for all that; refined, but quite colourless, except for the circumstance that at this moment it was blue with cold.
He held a bundle made up of an old faded silk handkerchief that apparently contained all his travelling wardrobe, and wore thick shoes and gaiters, his whole appearance being very un-Russian.
His black-haired neighbour inspected these peculiarities, having nothing better to do, and at length remarked, with that rude enjoyment of the discomforts of others which the common classes so often show:
“Cold?”
“Very,” said his neighbour, readily, “and this is a thaw, too. Fancy if it had been a hard frost! I never thought it would be so cold in the old country. I’ve grown quite out of the way of it.”
“What, been abroad, I suppose?”
“Yes, straight from Switzerland.”
“Wheugh! my goodness!” The black-haired young fellow whistled, and then laughed.
The conversation proceeded. The readiness of the fair-haired young man in the cloak to answer all his opposite neighbour’s questions was surprising.
He seemed to have no suspicion of any impertinence or inappropriateness in the fact of such questions being put to him.
Replying to them, he made known to the inquirer that he certainly had been long absent from Russia, more than four years; that he had been sent abroad for his health;
that he had suffered from some strange nervous malady—a kind of epilepsy, with convulsive spasms. His interlocutor burst out laughing several times at his answers; and more than ever, when to the question, “whether he had been cured?” the patient replied:
“No, they did not cure me.”
“Hey! that’s it! You stumped up your money for nothing, and we believe in those fellows, here!” remarked the black-haired individual, sarcastically.";

    const TEST_PARAGRAPH3: &str = "When the widow hurried away to Pavlofsk, she went straight to Daria Alexeyevna’s house, and telling all she knew, threw her into a state of great alarm.
Both ladies decided to communicate at once with Lebedeff, who, as the friend and landlord of the prince, was also much agitated.
Vera Lebedeff told all she knew, and by Lebedeff’s advice it was decided that all three should go to Petersburg as quickly as possible, in order to avert “what might so easily happen.”
This is how it came about that at eleven o’clock next morning Rogojin’s flat was opened by the police in the presence of Lebedeff, the two ladies, and Rogojin’s own brother, who lived in the wing.
The evidence of the porter went further than anything else towards the success of Lebedeff in gaining the assistance of the police.
He declared that he had seen Rogojin return to the house last night, accompanied by a friend, and that both had gone upstairs very secretly and cautiously.
After this there was no hesitation about breaking open the door, since it could not be got open in any other way.
Rogojin suffered from brain fever for two months. When he recovered from the attack he was at once brought up on trial for murder.
He gave full, satisfactory, and direct evidence on every point; and the prince’s name was, thanks to this, not brought into the proceedings.
Rogojin was very quiet during the progress of the trial. He did not contradict his clever and eloquent counsel, who argued that the brain fever,
or inflammation of the brain, was the cause of the crime; clearly proving that this malady had existed long before the murder was perpetrated, and had been brought on by the sufferings of the accused.
But Rogojin added no words of his own in confirmation of this view, and as before, he recounted with marvellous exactness the details of his crime.
He was convicted, but with extenuating circumstances, and condemned to hard labour in Siberia for fifteen years. He heard his sentence grimly, silently, and thoughtfully. His colossal fortune,
with the exception of the comparatively small portion wasted in the first wanton period of his inheritance, went to his brother, to the great satisfaction of the latter.
The old lady, Rogojin’s mother, is still alive, and remembers her favourite son Parfen sometimes, but not clearly. God spared her the knowledge of this dreadful calamity which had overtaken her house.
Lebedeff, Keller, Gania, Ptitsin, and many other friends of ours continue to live as before. There is scarcely any change in them, so that there is no need to tell of their subsequent doings.
Hippolyte died in great agitation, and rather sooner than he expected, about a fortnight after Nastasia Philipovna’s death. Colia was much affected by these events,
and drew nearer to his mother in heart and sympathy. Nina Alexandrovna is anxious, because he is “thoughtful beyond his years,” but he will, we think, make a useful and active man.
The prince’s further fate was more or less decided by Colia, who selected, out of all the persons he had met during the last six or seven months, Evgenie Pavlovitch, as friend and confidant.
To him he made over all that he knew as to the events above recorded, and as to the present condition of the prince. He was not far wrong in his choice.
Evgenie Pavlovitch took the deepest interest in the fate of the unfortunate “idiot,” and, thanks to his influence, the prince found himself once more with Dr. Schneider, in Switzerland.
Evgenie Pavlovitch, who went abroad at this time, intending to live a long while on the continent, being, as he often said, quite superfluous in Russia, visits his sick friend at Schneider’s every few months.
But Dr. Schneider frowns ever more and more and shakes his head; he hints that the brain is fatally injured; he does not as yet declare that his patient is incurable, but he allows himself to express the gravest fears.
Evgenie takes this much to heart, and he has a heart, as is proved by the fact that he receives and even answers letters from Colia. But besides this,
another trait in his character has become apparent, and as it is a good trait we will make haste to reveal it.
After each visit to Schneider’s establishment, Evgenie Pavlovitch writes another letter, besides that to Colia, giving the most minute particulars concerning the invalid’s condition.
In these letters is to be detected, and in each one more than the last, a growing feeling of friendship and sympathy.
The individual who corresponds thus with Evgenie Pavlovitch, and who engages so much of his attention and respect, is Vera Lebedeff.
We have never been able to discover clearly how such relations sprang up.
Of course the root of them was in the events which we have already recorded, and which so filled Vera with grief on the prince’s account that she fell seriously ill.
But exactly how the acquaintance and friendship came about, we cannot say.";

    #[test_log::test(tokio::test)]
    async fn test_symbol_new() {
        let st = SymbolTable::new();
        assert!(st.n_symbols == 0);
        for i in 0..=255_u8 {
            assert!(st.symbols[i as usize] == Symbol::from_char(i, i as u16));
        }
        let s = Symbol::from_char(1, 1);
        assert!(s == st.symbols[1]);
        for i in 0..FSST_HASH_TAB_SIZE {
            assert!(st.hash_tab[i] == Symbol::new());
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_fsst() {
        let test_input_size = 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH.len();
        let test_input = TEST_PARAGRAPH.repeat(repeat_num);
        helper(&test_input);

        let test_input_size = 2 * 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH.len();
        let test_input = TEST_PARAGRAPH.repeat(repeat_num);
        helper(&test_input);

        let test_input_size = 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH2.len();
        let test_input = TEST_PARAGRAPH.repeat(repeat_num);
        helper(&test_input);

        let test_input_size = 2 * 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH2.len();
        let test_input = TEST_PARAGRAPH2.repeat(repeat_num);
        helper(&test_input);

        let test_input_size = 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH3.len();
        let test_input = TEST_PARAGRAPH3.repeat(repeat_num); // Also corrected `repea_num` to `repeat_num`
        helper(&test_input);

        let test_input_size = 2 * 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH3.len();
        let test_input = TEST_PARAGRAPH3.repeat(repeat_num); // Also corrected `repea_num` to `repeat_num`
        helper(&test_input);
    }

    fn helper(test_input: &str) {
        let lines_vec = test_input.lines().collect::<Vec<&str>>();
        let string_array = StringArray::from(lines_vec);
        let mut compress_output_buf: Vec<u8> = vec![0; string_array.value_data().len()];
        let mut compress_offset_buf: Vec<i32> = vec![0; string_array.value_offsets().len()];
        let mut symbol_table = [0; FSST_SYMBOL_TABLE_SIZE];
        compress(
            symbol_table.as_mut(),
            string_array.value_data(),
            string_array.value_offsets(),
            &mut compress_output_buf,
            &mut compress_offset_buf,
        )
        .unwrap();
        let mut decompress_output: Vec<u8> = vec![0; compress_output_buf.len() * 8];
        let mut decompress_offsets: Vec<i32> = vec![0; compress_offset_buf.len()];
        decompress(
            &symbol_table,
            &compress_output_buf,
            &compress_offset_buf,
            &mut decompress_output,
            &mut decompress_offsets,
        )
        .unwrap();
        for i in 1..decompress_offsets.len() {
            let s = &decompress_output
                [decompress_offsets[i - 1] as usize..decompress_offsets[i] as usize];
            let original = &string_array.value_data()[string_array.value_offsets().to_vec()[i - 1]
                as usize
                ..string_array.value_offsets().to_vec()[i] as usize];
            assert!(
                s == original,
                "s: {:?}\n\n, original: {:?}",
                std::str::from_utf8(s),
                std::str::from_utf8(original)
            );
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_fsst_64_bit_offsets() {
        let test_input_size = 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH.len();
        let test_input = TEST_PARAGRAPH.repeat(repeat_num);
        helper_64_bit(&test_input);

        let test_input_size = 2 * 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH.len();
        let test_input = TEST_PARAGRAPH.repeat(repeat_num);
        helper_64_bit(&test_input);

        let test_input_size = 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH2.len();
        let test_input = TEST_PARAGRAPH2.repeat(repeat_num);
        helper_64_bit(&test_input);

        let test_input_size = 2 * 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH2.len();
        let test_input = TEST_PARAGRAPH2.repeat(repeat_num);
        helper_64_bit(&test_input);

        let test_input_size = 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH3.len();
        let test_input = TEST_PARAGRAPH3.repeat(repeat_num);
        helper_64_bit(&test_input);

        let test_input_size = 2 * 1024 * 1024;
        let repeat_num = test_input_size / TEST_PARAGRAPH3.len();
        let test_input = TEST_PARAGRAPH3.repeat(repeat_num);
        helper_64_bit(&test_input);
    }

    fn helper_64_bit(test_input: &str) {
        use arrow_array::LargeStringArray;
        let lines_vec = test_input.lines().collect::<Vec<&str>>();
        let string_array = LargeStringArray::from(lines_vec);
        let mut compress_output_buf: Vec<u8> = vec![0; string_array.value_data().len()];
        let mut compress_offset_buf: Vec<i64> = vec![0; string_array.value_offsets().len()];
        let mut symbol_table = [0; FSST_SYMBOL_TABLE_SIZE];
        compress(
            symbol_table.as_mut(),
            string_array.value_data(),
            string_array.value_offsets(),
            &mut compress_output_buf,
            &mut compress_offset_buf,
        )
        .unwrap();
        let mut decompress_output: Vec<u8> = vec![0; compress_output_buf.len() * 8];
        let mut decompress_offsets: Vec<i64> = vec![0; compress_offset_buf.len()];
        decompress(
            &symbol_table,
            &compress_output_buf,
            &compress_offset_buf,
            &mut decompress_output,
            &mut decompress_offsets,
        )
        .unwrap();
        for i in 1..decompress_offsets.len() {
            let s = &decompress_output
                [decompress_offsets[i - 1] as usize..decompress_offsets[i] as usize];
            let original = &string_array.value_data()[string_array.value_offsets().to_vec()[i - 1]
                as usize
                ..string_array.value_offsets().to_vec()[i] as usize];
            assert!(
                s == original,
                "s: {:?}\n\n, original: {:?}",
                std::str::from_utf8(s),
                std::str::from_utf8(original)
            );
        }
    }

    // Build a genuinely FSST-compressed (decoder_switch_on) buffer to exercise the decode-side
    // output-buffer size contract. Returns (symbol_table, compressed_bytes, compressed_offsets).
    fn compress_paragraph() -> ([u8; FSST_SYMBOL_TABLE_SIZE], Vec<u8>, Vec<i32>) {
        let test_input = TEST_PARAGRAPH.repeat((1024 * 1024) / TEST_PARAGRAPH.len());
        let lines_vec = test_input.lines().collect::<Vec<&str>>();
        let string_array = StringArray::from(lines_vec);
        let mut compress_output_buf: Vec<u8> = vec![0; string_array.value_data().len()];
        let mut compress_offset_buf: Vec<i32> = vec![0; string_array.value_offsets().len()];
        let mut symbol_table = [0; FSST_SYMBOL_TABLE_SIZE];
        compress(
            symbol_table.as_mut(),
            string_array.value_data(),
            string_array.value_offsets(),
            &mut compress_output_buf,
            &mut compress_offset_buf,
        )
        .unwrap();
        (symbol_table, compress_output_buf, compress_offset_buf)
    }

    // The decoder writes a full 8-byte word per code, so the output buffer must be at least 8x the
    // compressed input. A buffer sized below 8x must be rejected rather than written out of bounds.
    #[test_log::test(tokio::test)]
    async fn test_decompress_rejects_undersized_output_buffer() {
        let (symbol_table, compressed, compressed_offsets) = compress_paragraph();
        // Sanity check: this input actually engaged FSST compression (decoder_switch_on).
        let st_info = u64::from_ne_bytes(symbol_table[..8].try_into().unwrap());
        assert!(st_info & (1 << 24) != 0, "expected decoder_switch_on input");

        // One byte short of the 8x requirement must be rejected.
        let mut too_small = vec![0u8; compressed.len() * 8 - 1];
        let mut out_offsets = vec![0i32; compressed_offsets.len()];
        let err = decompress(
            &symbol_table,
            &compressed,
            &compressed_offsets,
            &mut too_small,
            &mut out_offsets,
        )
        .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        // Exactly 8x is the tight bound and must succeed.
        let mut exact = vec![0u8; compressed.len() * 8];
        let mut out_offsets = vec![0i32; compressed_offsets.len()];
        decompress(
            &symbol_table,
            &compressed,
            &compressed_offsets,
            &mut exact,
            &mut out_offsets,
        )
        .unwrap();
    }
}
