//! The derive's own refusals, as compile errors with useful spans.
#[test]
fn derive_errors() {
    trybuild::TestCases::new().compile_fail("tests/ui/*.rs");
}
