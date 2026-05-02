//! Unicode-aware tokenizer mirroring SQLite's `unicode61` defaults.
//!
//! Pipeline, in order:
//!
//! 1. **NFKD normalize** — decomposes accented characters into base
//!    letter + combining mark so the next step can strip the marks.
//! 2. **UAX #29 word segmentation** — splits the text into "words"
//!    that respect locale-agnostic Unicode rules. Punctuation and
//!    whitespace fall out as separate (non-word) segments which we
//!    drop.
//! 3. **Lowercase** — folds case via `to_lowercase()` (Unicode-aware).
//! 4. **Strip combining marks** — drops characters in the `Mn`
//!    (nonspacing mark) general category, so `café` → `cafe`.
//! 5. **Drop empty / pure-non-alphanumeric tokens** — anything left
//!    after stripping that has no alphanumeric content (segmenter
//!    occasionally hands back fragments of punctuation when fed
//!    boundary cases) is filtered out.
//!
//! Positions are 0-indexed and stable across the full input — every
//! word boundary advances the counter, even when the token is dropped
//! by the filter, so the position numbers reflect the raw word stream
//! and surface-form proximity (used by `NEAR(...)`) stays meaningful.

use unicode_normalization::UnicodeNormalization;
use unicode_normalization::char::is_combining_mark;
use unicode_segmentation::UnicodeSegmentation;

/// One emitted token: the cleaned-up text plus its 0-indexed position
/// in the source word stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Token {
    pub text: String,
    pub position: u32,
}

/// Tokenize `input` per the rules described at the module level.
pub fn tokenize(input: &str) -> Vec<Token> {
    let mut out = Vec::new();
    let mut position: u32 = 0;
    for word in input.unicode_words() {
        let cleaned = clean(word);
        if !cleaned.is_empty() {
            out.push(Token {
                text: cleaned,
                position,
            });
        }
        position = position.saturating_add(1);
    }
    out
}

/// Clean a single word slice: NFKD-decompose, lowercase, strip
/// combining marks, then strip any non-alphanumeric leftovers (the
/// segmenter occasionally hands back fragments like `'s` whose
/// apostrophe survives normalization).
pub(crate) fn clean(word: &str) -> String {
    let mut out = String::with_capacity(word.len());
    for ch in word.nfkd() {
        if is_combining_mark(ch) {
            continue;
        }
        for lc in ch.to_lowercase() {
            if lc.is_alphanumeric() {
                out.push(lc);
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn texts(tokens: &[Token]) -> Vec<&str> {
        tokens.iter().map(|t| t.text.as_str()).collect()
    }

    #[test]
    fn lowercases_and_splits_on_punctuation() {
        let toks = tokenize("The Quick BROWN-fox!");
        assert_eq!(texts(&toks), vec!["the", "quick", "brown", "fox"]);
    }

    #[test]
    fn whitespace_only_yields_no_tokens() {
        assert!(tokenize("   ").is_empty());
        assert!(tokenize("").is_empty());
    }

    #[test]
    fn strips_diacritics() {
        let toks = tokenize("café Olé naïve");
        assert_eq!(texts(&toks), vec!["cafe", "ole", "naive"]);
    }

    #[test]
    fn handles_unicode_punctuation() {
        // `Hello, world! Olé.` → `["hello", "world", "ole"]`.
        let toks = tokenize("Hello, world! Olé.");
        assert_eq!(texts(&toks), vec!["hello", "world", "ole"]);
    }

    #[test]
    fn positions_are_zero_indexed_and_stable() {
        let toks = tokenize("alpha beta gamma");
        let positions: Vec<u32> = toks.iter().map(|t| t.position).collect();
        assert_eq!(positions, vec![0, 1, 2]);
    }

    #[test]
    fn cjk_yields_per_word_tokens() {
        // Unicode segmentation treats each CJK ideograph as a word.
        let toks = tokenize("日本語のテスト");
        assert!(!toks.is_empty());
    }

    #[test]
    fn numbers_kept_as_alphanumeric() {
        // Unicode word-segmentation keeps decimal numbers intact, so "4.5"
        // is one token. Plain integers are unaffected.
        let toks = tokenize("123 abc 4.5");
        assert_eq!(texts(&toks), vec!["123", "abc", "45"]);
    }

    #[test]
    fn case_insensitive_unicode() {
        let toks = tokenize("ÜBER über");
        assert_eq!(texts(&toks), vec!["uber", "uber"]);
    }
}
