/// Parsed search box: free text plus `tag:`, `todo:`, `notes:`, `ext:` filters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchQuery {
    pub text: Vec<String>,
    pub tags: Vec<String>,
    pub todos: Vec<String>,
    pub notes: Vec<String>,
    pub extensions: Vec<String>,
}

pub fn parse_search(input: &str) -> SearchQuery {
    let mut q = SearchQuery::default();
    for token in input.split_whitespace() {
        if let Some(rest) = token.strip_prefix("tag:") {
            if !rest.is_empty() {
                q.tags.push(rest.to_lowercase());
            }
        } else if let Some(rest) = token.strip_prefix("todo:") {
            if !rest.is_empty() {
                q.todos.push(rest.to_lowercase());
            }
        } else if let Some(rest) = token.strip_prefix("notes:") {
            if !rest.is_empty() {
                q.notes.push(rest.to_lowercase());
            }
        } else if let Some(rest) = token.strip_prefix("ext:") {
            if !rest.is_empty() {
                let ext = rest.trim_start_matches('.').to_lowercase();
                q.extensions.push(ext);
            }
        } else {
            q.text.push(token.to_lowercase());
        }
    }
    q
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_mixed_query() {
        let q = parse_search("report tag:work todo:review notes:draft ext:pdf Q4");
        assert_eq!(q.text, vec!["report", "q4"]);
        assert_eq!(q.tags, vec!["work"]);
        assert_eq!(q.todos, vec!["review"]);
        assert_eq!(q.notes, vec!["draft"]);
        assert_eq!(q.extensions, vec!["pdf"]);
    }
}
