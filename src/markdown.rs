lazy_static::lazy_static! {
    // This is more restrictive than the WebFinger spec but probably okay?
    pub static ref MENTION_REGEX: regex::Regex = regex::Regex::new(r"@([A-Za-z0-9-_~.]+)@([A-Za-z0-9-_~.]+(?::[0-9]+)?)").unwrap();
}

pub fn parse_markdown(src: &str) -> impl Iterator<Item = pulldown_cmark::Event> {
    let parser = pulldown_cmark::Parser::new(src);

    let stream = pdcm_linkify::AutoLinker::new(parser);

    stream
}

pub fn render_markdown_from_stream<'a>(
    stream: impl Iterator<Item = pulldown_cmark::Event<'a>>,
) -> String {
    let mut output = String::new();
    pulldown_cmark::html::push_html(&mut output, stream);

    output
}

pub fn render_markdown_simple(src: &str) -> String {
    render_markdown_from_stream(parse_markdown(src))
}
