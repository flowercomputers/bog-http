//! The OpenAPI 3.1 document, generated from the same [`Describe`]s that
//! mount the router.

use serde_json::{Map, Value, json};

use crate::resource::{Describe, Method, Route};
use crate::server::MIN_EPOCH_HEADER;
use crate::tx::MAX_TX_OPS;
use crate::wire::EPOCH_HEADER;

pub(crate) struct Info<'a> {
    pub title: &'a str,
    pub version: &'a str,
    pub description: Option<&'a str>,
}

/// The whole document. `schemas` are the shared components every
/// resource's `$ref`s point into.
pub(crate) fn document(
    info: &Info<'_>,
    resources: &[Describe],
    custom: &[Route],
    schemas: Map<String, Value>,
) -> Value {
    let mut paths = Map::new();
    let mut put = |path: String, r: &Route, op: Value| {
        let item = paths
            .entry(path)
            .or_insert_with(|| Value::Object(Map::new()));
        item[r.method.as_str()] = op;
    };
    for d in resources {
        for r in &d.routes {
            let id = format!("{}_{}", d.name, r.op);
            put(
                format!("/{}{}", d.name, r.path),
                r,
                operation(&id, &d.name, d.description.as_deref(), r),
            );
        }
    }
    for r in custom {
        let id = format!(
            "{}{}",
            r.method.as_str(),
            r.path.replace(|c: char| !c.is_ascii_alphanumeric(), "_")
        );
        put(r.path.to_string(), r, operation(&id, "custom", None, r));
    }
    paths.insert("/_tx".into(), transaction(resources));
    meta_paths(&mut paths);

    let mut info_obj = json!({ "title": info.title, "version": info.version });
    if let Some(description) = info.description {
        info_obj["description"] = json!(description);
    }
    let tags: Vec<Value> = resources
        .iter()
        .map(|d| {
            let mut tag = json!({ "name": d.name });
            if let Some(description) = &d.description {
                tag["description"] = json!(description);
            }
            tag
        })
        .chain([
            json!({ "name": "custom", "description": "Application routes." }),
            json!({ "name": "transactions", "description": "Many writes as one transaction." }),
            json!({ "name": "meta", "description": "Introspection and health." }),
        ])
        .collect();

    json!({
        "openapi": "3.1.0",
        "info": info_obj,
        // Relative: wherever the router is mounted.
        "servers": [{ "url": "/" }],
        "tags": tags,
        "paths": paths,
        "components": {
            "schemas": schemas,
            "responses": {
                "BadRequest": problem("A malformed key, query, cursor, or body."),
                "NotFound": problem("Nothing at this key, or no such route."),
                "UnsupportedMediaType": problem("The body isn't `application/json`."),
                "Rejected": problem("A schema constraint refused the write; `at` names the source or view that refused. Nothing was applied."),
                "Unavailable": problem("Try again later (`Retry-After`): too many pending writes (`overloaded`), a `fold-min-epoch` not reached in time (`epoch-not-reached`), or the store failing (`unavailable`)."),
                "PayloadTooLarge": problem("The body exceeds the server's limit."),
            },
            "parameters": {
                "MinEpoch": {
                    "name": MIN_EPOCH_HEADER,
                    "in": "header",
                    "required": false,
                    "description": "Answer from a snapshot at or after this epoch, waiting briefly for it: read your own writes by passing the epoch a write returned.",
                    "schema": { "type": "integer", "format": "uint64", "minimum": 0 },
                },
            },
            "headers": {
                "FoldEpoch": {
                    "description": "The epoch of the snapshot read, or the first epoch that includes the write.",
                    "schema": { "type": "integer", "format": "uint64", "minimum": 0 },
                },
            },
        },
    })
}

fn operation(id: &str, tag: &str, description: Option<&str>, r: &Route) -> Value {
    let mut op = json!({
        "operationId": id,
        "summary": r.summary,
        "tags": [tag],
    });
    if let Some(description) = description {
        op["description"] = json!(description);
    }

    let mut params = Vec::new();
    if let Some(key) = &r.key {
        params.push(json!({
            "name": "key",
            "in": "path",
            "required": true,
            "description": "The key as JSON (`42`, `[1,\"a\"]`); string keys may also be written bare.",
            "schema": key,
        }));
    }
    if let Some(query) = &r.query {
        params.extend(query_params(query));
    }
    if r.method.is_read() {
        params.push(json!({ "$ref": "#/components/parameters/MinEpoch" }));
    }
    if !params.is_empty() {
        op["parameters"] = Value::Array(params);
    }

    if let Some(body) = &r.body {
        op["requestBody"] = json!({
            "required": true,
            "content": { "application/json": { "schema": body } },
        });
    }

    let mut responses = Map::new();
    let ok = success(&r.response);
    if r.creates {
        responses.insert("201".into(), ok.clone());
    }
    // A POST that creates always creates; a PUT may only replace.
    if !(r.creates && r.method == Method::Post) {
        responses.insert("200".into(), ok);
    }
    let mut refer = |code: &str, name: &str| {
        responses.insert(
            code.into(),
            json!({ "$ref": format!("#/components/responses/{name}") }),
        );
    };
    refer("400", "BadRequest");
    if r.may_404 {
        refer("404", "NotFound");
    }
    if r.body.is_some() {
        refer("413", "PayloadTooLarge");
        refer("415", "UnsupportedMediaType");
    }
    if !r.method.is_read() {
        refer("422", "Rejected");
    }
    refer("503", "Unavailable");
    op["responses"] = Value::Object(responses);
    op
}

/// `POST /_tx`: its body is an array of every write route, addressed by
/// resource and op, as a `oneOf` clients can generate types from.
fn transaction(resources: &[Describe]) -> Value {
    let mut variants = Vec::new();
    for d in resources {
        for r in d.routes.iter().filter(|r| !r.method.is_read()) {
            let mut props = Map::new();
            let mut required = vec![json!("resource"), json!("op")];
            props.insert("resource".into(), json!({ "const": d.name }));
            props.insert("op".into(), json!({ "const": r.op }));
            if let Some(key) = &r.key {
                props.insert("key".into(), key.clone());
                required.push(json!("key"));
            }
            if let Some(body) = &r.body {
                props.insert("body".into(), body.clone());
                required.push(json!("body"));
            }
            if let Some(query) = &r.query {
                let mut query = query.clone();
                if let Some(o) = query.as_object_mut() {
                    o.remove("title");
                    o.remove("$schema");
                }
                props.insert("query".into(), query);
            }
            variants.push(json!({
                "title": format!("{}_{}", d.name, r.op),
                "description": r.summary,
                "type": "object",
                "required": required,
                "properties": props,
                "additionalProperties": false,
            }));
        }
    }
    let results = json!({
        "type": "object",
        "required": ["results"],
        "properties": {
            "results": {
                "type": "array",
                "description": "Each operation's `data`, in order.",
            },
        },
    });
    let mut op = json!({
        "operationId": "transaction",
        "summary": "Apply many writes, across any sources, as one transaction",
        "description": format!("All or nothing: one epoch, and any refusal or malformed operation applies none of them (a malformed one is named by `index`). Each operation sees the ones before it. At most {MAX_TX_OPS} operations."),
        "tags": ["transactions"],
        "requestBody": {
            "required": true,
            "content": {
                "application/json": {
                    "schema": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": MAX_TX_OPS,
                        "items": { "oneOf": variants },
                    },
                },
            },
        },
        "responses": {
            "200": success(&results),
        },
    });
    for (code, name) in [
        ("400", "BadRequest"),
        ("413", "PayloadTooLarge"),
        ("415", "UnsupportedMediaType"),
        ("422", "Rejected"),
        ("503", "Unavailable"),
    ] {
        op["responses"][code] = json!({ "$ref": format!("#/components/responses/{name}") });
    }
    json!({ "post": op })
}

/// A success response: the `{epoch, data}` envelope around `data`.
fn success(data: &Value) -> Value {
    json!({
        "description": "Success.",
        "headers": { EPOCH_HEADER: { "$ref": "#/components/headers/FoldEpoch" } },
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "required": ["epoch", "data"],
                    "properties": {
                        "epoch": { "type": "integer", "format": "uint64", "minimum": 0 },
                        "data": data,
                    },
                },
            },
        },
    })
}

fn problem(description: &str) -> Value {
    json!({
        "description": description,
        "content": {
            "application/problem+json": {
                "schema": { "$ref": "#/components/schemas/Problem" },
            },
        },
    })
}

/// One OpenAPI parameter per property of an inline object schema.
fn query_params(schema: &Value) -> Vec<Value> {
    let required: Vec<&str> = schema["required"]
        .as_array()
        .map(|r| r.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let Some(props) = schema["properties"].as_object() else {
        return Vec::new();
    };
    props
        .iter()
        .map(|(name, prop)| {
            let mut prop = prop.clone();
            let description = prop.as_object_mut().and_then(|o| o.remove("description"));
            strip_null(&mut prop);
            let mut param = json!({
                "name": name,
                "in": "query",
                "required": required.contains(&name.as_str()),
                "schema": prop,
            });
            if let Some(description) = description {
                param["description"] = description;
            }
            param
        })
        .collect()
}

/// An absent query parameter is how `null` is spelled; drop it from the
/// parameter's type.
fn strip_null(schema: &mut Value) {
    if let Some(Value::Array(types)) = schema.get_mut("type") {
        types.retain(|t| t != "null");
        if types.len() == 1 {
            schema["type"] = types[0].clone();
        }
    }
}

fn meta_paths(paths: &mut Map<String, Value>) {
    let meta = |id: &str, summary: &str| {
        json!({
            "get": {
                "operationId": id,
                "summary": summary,
                "tags": ["meta"],
                "responses": {
                    "200": {
                        "description": "Success.",
                        "content": { "application/json": { "schema": { "type": "object" } } },
                    },
                },
            },
        })
    };
    paths.insert(
        "/_schema".into(),
        meta("schema", "Describe every resource: kind, schemas, routes"),
    );
    paths.insert("/_health".into(), meta("health", "Liveness"));
    paths.insert("/openapi.json".into(), meta("openapi", "This document"));
}
