use std::borrow::Cow;

pub struct Translator {
    bundle: fluent::concurrent::FluentBundle<&'static fluent::FluentResource>,
}
impl Translator {
    pub fn new(
        bundle: fluent::concurrent::FluentBundle<&'static fluent::FluentResource>,
    ) -> Translator {
        Translator { bundle }
    }

    pub fn tr<'a>(&'a self, input: &'a (impl ToKeyAndArgs + 'a)) -> Cow<'a, str> {
        let (key, args) = input.to_key_and_args();

        let mut errors = Vec::with_capacity(0);
        let out = match self.bundle.get_message(key) {
            Some(msg) => self.bundle.format_pattern(
                msg.value.expect("Missing value for translation key"),
                args,
                &mut errors,
            ),
            None => {
                log::error!("Missing translation for {}", key);
                Cow::Borrowed(key)
            }
        };
        if !errors.is_empty() {
            log::error!("Errors in translation: {:?}", errors);
        }

        out
    }
}

pub trait ToKeyAndArgs {
    fn to_key_and_args(&self) -> (&str, Option<&fluent::FluentArgs>);
}

pub struct PlainLangKey(&'static str);
impl ToKeyAndArgs for PlainLangKey {
    fn to_key_and_args(&self) -> (&str, Option<&fluent::FluentArgs>) {
        (self.0, None)
    }
}

pub struct LangKeyWithArgs<'a>(&'static str, fluent::FluentArgs<'a>);
impl<'a> ToKeyAndArgs for LangKeyWithArgs<'a> {
    fn to_key_and_args(&self) -> (&str, Option<&fluent::FluentArgs>) {
        (self.0, Some(&self.1))
    }
}

include!(concat!(env!("OUT_DIR"), "/lang_keys.rs"));
