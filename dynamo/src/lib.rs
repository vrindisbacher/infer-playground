use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;
use flux_rs::attrs::*;

extern crate flux_alloc;

#[reflect]
enum MySort {
    String,
    Number,
    Bool,
    WhoCares,
}

// TODO: Is there a better way to refine this????
#[flux_rs::refined_by(
    kind: MySort,
    str_val: str,
    bool_val: bool
)]
pub enum AttributeVal {
    #[variant((_) -> AttributeVal[MySort::WhoCares, "", false])]
    B(aws_sdk_dynamodb::primitives::Blob),
    #[variant((bool[@b]) -> AttributeVal[MySort::Bool, "", b])]
    Bool(bool),
    #[variant((_) -> AttributeVal[MySort::WhoCares, "", false])]
    Bs(std::vec::Vec<aws_sdk_dynamodb::primitives::Blob>),
    #[variant((_) -> AttributeVal[MySort::WhoCares, "", false])]
    L(std::vec::Vec<aws_sdk_dynamodb::types::AttributeValue>),
    #[variant((_) -> AttributeVal[MySort::WhoCares, "", false])]
    M(std::collections::HashMap<std::string::String, aws_sdk_dynamodb::types::AttributeValue>),
    #[variant((String[@s]) -> AttributeVal[MySort::Number, s, false])]
    N(String),
    #[variant((_) -> AttributeVal[MySort::WhoCares, "", false])]
    Ns(std::vec::Vec<std::string::String>),
    #[variant((_) -> AttributeVal[MySort::WhoCares, "", false])]
    Null(bool),
    #[variant((String[@s]) -> AttributeVal[MySort::String, s, false])]
    S(std::string::String),
    #[variant((_) -> AttributeVal[MySort::WhoCares, "", false])]
    Ss(std::vec::Vec<::std::string::String>),
    #[variant(AttributeVal[MySort::WhoCares, "", false])]
    #[non_exhaustive]
    Unknown,
}

impl From<AttributeValue> for AttributeVal {
    fn from(value: AttributeValue) -> Self {
        match value {
            AttributeValue::B(blob) => AttributeVal::B(blob),
            AttributeValue::Bool(b) => AttributeVal::Bool(b),
            AttributeValue::Bs(blobs) => AttributeVal::Bs(blobs),
            AttributeValue::L(attribute_values) => AttributeVal::L(attribute_values),
            AttributeValue::M(hash_map) => AttributeVal::M(hash_map),
            AttributeValue::N(n) => AttributeVal::N(n),
            AttributeValue::Ns(items) => AttributeVal::Ns(items),
            AttributeValue::Null(b) => AttributeVal::Null(b),
            AttributeValue::S(s) => AttributeVal::S(s),
            AttributeValue::Ss(items) => AttributeVal::Ss(items),
            _ => AttributeVal::Unknown,
        }
    }
}

#[trusted]
impl From<AttributeVal> for aws_sdk_dynamodb::types::AttributeValue {
    fn from(val: AttributeVal) -> Self {
        match val {
            AttributeVal::B(blob) => aws_sdk_dynamodb::types::AttributeValue::B(blob),
            AttributeVal::Bool(b) => aws_sdk_dynamodb::types::AttributeValue::Bool(b),
            AttributeVal::Bs(blobs) => aws_sdk_dynamodb::types::AttributeValue::Bs(blobs),
            AttributeVal::L(attribute_values) => {
                aws_sdk_dynamodb::types::AttributeValue::L(attribute_values)
            }
            AttributeVal::M(hash_map) => aws_sdk_dynamodb::types::AttributeValue::M(hash_map),
            AttributeVal::N(n) => aws_sdk_dynamodb::types::AttributeValue::N(n),
            AttributeVal::Ns(items) => aws_sdk_dynamodb::types::AttributeValue::Ns(items),
            AttributeVal::Null(b) => aws_sdk_dynamodb::types::AttributeValue::Null(b),
            AttributeVal::S(s) => aws_sdk_dynamodb::types::AttributeValue::S(s),
            AttributeVal::Ss(items) => aws_sdk_dynamodb::types::AttributeValue::Ss(items),
            AttributeVal::Unknown => todo!(),
        }
    }
}

// ==========================================================================
// Client
// ==========================================================================

pub struct DynamoClient {
    inner: aws_sdk_dynamodb::Client,
}

#[trusted]
impl DynamoClient {
    #[sig(fn(aws_sdk_dynamodb::Client) -> DynamoClient)]
    pub fn new(inner: aws_sdk_dynamodb::Client) -> Self {
        Self { inner }
    }

    #[sig(fn(&DynamoClient) -> GetItemBuilder[false, false])]
    pub fn get_item(&self) -> GetItemBuilder {
        GetItemBuilder {
            inner: self.inner.get_item(),
        }
    }

    #[sig(fn(&DynamoClient) -> PutItemBuilder)]
    pub fn put_item(&self) -> PutItemBuilder {
        PutItemBuilder {
            inner: self.inner.put_item(),
        }
    }

    #[sig(fn(&DynamoClient) -> DeleteItemBuilder[false, false])]
    pub fn delete_item(&self) -> DeleteItemBuilder {
        DeleteItemBuilder {
            inner: self.inner.delete_item(),
        }
    }

    #[sig(fn(&DynamoClient) -> UpdateItemBuilder[false, false])]
    pub fn update_item(&self) -> UpdateItemBuilder {
        UpdateItemBuilder {
            inner: self.inner.update_item(),
        }
    }

    #[sig(fn(&DynamoClient) -> QueryBuilder[false, false])]
    pub fn query(&self) -> QueryBuilder {
        QueryBuilder {
            inner: self.inner.query(),
        }
    }
}

// ==========================================================================
// GetItem
// ==========================================================================

#[opaque]
#[refined_by(has_table: bool, has_key: bool)]
pub struct GetItemBuilder {
    inner: aws_sdk_dynamodb::operation::get_item::builders::GetItemFluentBuilder,
}

#[trusted]
impl GetItemBuilder {
    #[sig(fn(GetItemBuilder[@b], _) -> GetItemBuilder[true, b.has_key])]
    pub fn table_name(self, name: impl Into<String>) -> Self {
        Self {
            inner: self.inner.table_name(name),
        }
    }

    #[sig(fn(GetItemBuilder[@b], _, AttributeVal) -> GetItemBuilder[b.has_table, true])]
    pub fn key(self, key: impl Into<String>, value: AttributeVal) -> Self {
        Self {
            inner: self.inner.key(key, value.into()),
        }
    }

    #[sig(fn(GetItemBuilder[@b], HashMap<String, AttributeVal>) -> GetItemBuilder[b.has_table, true])]
    pub fn set_key(self, key: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            key.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            inner: self.inner.set_key(Some(converted)),
        }
    }

    #[sig(fn(GetItemBuilder[true, true]) -> _)]
    pub async fn send(
        self,
    ) -> Result<
        GetItemOutput,
        aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::get_item::GetItemError>,
    > {
        let result = self.inner.send().await?;
        Ok(GetItemOutput { inner: result })
    }
}

#[opaque]
pub struct GetItemOutput {
    inner: aws_sdk_dynamodb::operation::get_item::GetItemOutput,
}

#[trusted]
impl GetItemOutput {
    #[sig(fn(&GetItemOutput) -> bool)]
    pub fn has_item(&self) -> bool {
        self.inner.item.is_some()
    }

    #[sig(fn(GetItemOutput) -> Option<HashMap<String, AttributeVal>>)]
    pub fn into_item(self) -> Option<HashMap<String, AttributeVal>> {
        self.inner
            .item
            .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

// ==========================================================================
// PutItem
// ==========================================================================

#[opaque]
#[refined_by(table_name: str, items: Map<str, AttributeVal>)]
pub struct PutItemBuilder {
    inner: aws_sdk_dynamodb::operation::put_item::builders::PutItemFluentBuilder,
}

#[trusted]
impl PutItemBuilder {
    #[sig(fn(PutItemBuilder[@b], String[@s]) -> PutItemBuilder[s, b.items])]
    pub fn table_name(self, name: String) -> Self {
        Self {
            inner: self.inner.table_name(name),
        }
    }

    #[sig(fn(PutItemBuilder[@b], String[@s], AttributeVal[@v]) -> PutItemBuilder[{ items: map_store(b.items, s, v), ..b }])]
    pub fn item(self, key: String, value: AttributeVal) -> Self {
        Self {
            inner: self.inner.item(key, value.into()),
        }
    }

    #[sig(fn(PutItemBuilder[@b], HashMap<String, AttributeVal>[@m]) -> PutItemBuilder{b_new: b_new.table_name == b.table_name })]
    pub fn set_item(self, item: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            item.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            inner: self.inner.set_item(Some(converted)),
        }
    }

    #[sink(DynamoPut)]
    #[sig(fn(PutItemBuilder) -> _)]
    pub async fn send(
        self,
    ) -> Result<
        PutItemOutput,
        aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::put_item::PutItemError>,
    > {
        let result = self.inner.send().await?;
        Ok(PutItemOutput { inner: result })
    }
}

pub struct PutItemOutput {
    inner: aws_sdk_dynamodb::operation::put_item::PutItemOutput,
}

#[trusted]
impl PutItemOutput {
    pub fn into_attributes(self) -> Option<HashMap<String, AttributeVal>> {
        self.inner
            .attributes
            .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

// ==========================================================================
// DeleteItem
// ==========================================================================

#[opaque]
#[refined_by(has_table: bool, has_key: bool)]
pub struct DeleteItemBuilder {
    inner: aws_sdk_dynamodb::operation::delete_item::builders::DeleteItemFluentBuilder,
}

#[trusted]
impl DeleteItemBuilder {
    #[sig(fn(DeleteItemBuilder[@b], _) -> DeleteItemBuilder[true, b.has_key])]
    pub fn table_name(self, name: impl Into<String>) -> Self {
        Self {
            inner: self.inner.table_name(name),
        }
    }

    #[sig(fn(DeleteItemBuilder[@b], _, AttributeVal) -> DeleteItemBuilder[b.has_table, true])]
    pub fn key(self, key: impl Into<String>, value: AttributeVal) -> Self {
        Self {
            inner: self.inner.key(key, value.into()),
        }
    }

    #[sig(fn(DeleteItemBuilder[@b], HashMap<String, AttributeVal>) -> DeleteItemBuilder[b.has_table, true])]
    pub fn set_key(self, key: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            key.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            inner: self.inner.set_key(Some(converted)),
        }
    }

    #[sig(fn(DeleteItemBuilder[true, true]) -> _)]
    pub async fn send(
        self,
    ) -> Result<
        DeleteItemOutput,
        aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::delete_item::DeleteItemError,
        >,
    > {
        let result = self.inner.send().await?;
        Ok(DeleteItemOutput { inner: result })
    }
}

#[opaque]
pub struct DeleteItemOutput {
    inner: aws_sdk_dynamodb::operation::delete_item::DeleteItemOutput,
}

#[trusted]
impl DeleteItemOutput {
    #[sig(fn(DeleteItemOutput) -> Option<HashMap<String, AttributeVal>>)]
    pub fn into_attributes(self) -> Option<HashMap<String, AttributeVal>> {
        self.inner
            .attributes
            .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

// ==========================================================================
// UpdateItem
// ==========================================================================

#[opaque]
#[refined_by(has_table: bool, has_key: bool)]
pub struct UpdateItemBuilder {
    inner: aws_sdk_dynamodb::operation::update_item::builders::UpdateItemFluentBuilder,
}

#[trusted]
impl UpdateItemBuilder {
    #[sig(fn(UpdateItemBuilder[@b], _) -> UpdateItemBuilder[true, b.has_key])]
    pub fn table_name(self, name: impl Into<String>) -> Self {
        Self {
            inner: self.inner.table_name(name),
        }
    }

    #[sig(fn(UpdateItemBuilder[@b], _, AttributeVal) -> UpdateItemBuilder[b.has_table, true])]
    pub fn key(self, key: impl Into<String>, value: AttributeVal) -> Self {
        Self {
            inner: self.inner.key(key, value.into()),
        }
    }

    #[sig(fn(UpdateItemBuilder[@b], HashMap<String, AttributeVal>) -> UpdateItemBuilder[b.has_table, true])]
    pub fn set_key(self, key: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            key.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            inner: self.inner.set_key(Some(converted)),
        }
    }

    #[sig(fn(UpdateItemBuilder[@b], _) -> UpdateItemBuilder[b.has_table, b.has_key])]
    pub fn update_expression(self, expr: impl Into<String>) -> Self {
        Self {
            inner: self.inner.update_expression(expr),
        }
    }

    #[sig(fn(UpdateItemBuilder[@b], _, _) -> UpdateItemBuilder[b.has_table, b.has_key])]
    pub fn expression_attribute_names(
        self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            inner: self.inner.expression_attribute_names(key, value),
        }
    }

    #[sig(fn(UpdateItemBuilder[@b], _, AttributeVal) -> UpdateItemBuilder[b.has_table, b.has_key])]
    pub fn expression_attribute_values(self, key: impl Into<String>, value: AttributeVal) -> Self {
        Self {
            inner: self.inner.expression_attribute_values(key, value.into()),
        }
    }

    #[sig(fn(UpdateItemBuilder[true, true]) -> _)]
    pub async fn send(
        self,
    ) -> Result<
        UpdateItemOutput,
        aws_sdk_dynamodb::error::SdkError<
            aws_sdk_dynamodb::operation::update_item::UpdateItemError,
        >,
    > {
        let result = self.inner.send().await?;
        Ok(UpdateItemOutput { inner: result })
    }
}

#[opaque]
pub struct UpdateItemOutput {
    inner: aws_sdk_dynamodb::operation::update_item::UpdateItemOutput,
}

#[trusted]
impl UpdateItemOutput {
    #[sig(fn(UpdateItemOutput) -> Option<HashMap<String, AttributeVal>>)]
    pub fn into_attributes(self) -> Option<HashMap<String, AttributeVal>> {
        self.inner
            .attributes
            .map(|m| m.into_iter().map(|(k, v)| (k, v.into())).collect())
    }
}

// ==========================================================================
// Query
// ==========================================================================

#[opaque]
#[refined_by(has_table: bool, has_key_condition: bool)]
pub struct QueryBuilder {
    inner: aws_sdk_dynamodb::operation::query::builders::QueryFluentBuilder,
}

#[trusted]
impl QueryBuilder {
    #[sig(fn(QueryBuilder[@b], _) -> QueryBuilder[true, b.has_key_condition])]
    pub fn table_name(self, name: impl Into<String>) -> Self {
        Self {
            inner: self.inner.table_name(name),
        }
    }

    #[sig(fn(QueryBuilder[@b], _) -> QueryBuilder[b.has_table, b.has_key_condition])]
    pub fn index_name(self, name: impl Into<String>) -> Self {
        Self {
            inner: self.inner.index_name(name),
        }
    }

    #[sig(fn(QueryBuilder[@b], _) -> QueryBuilder[b.has_table, true])]
    pub fn key_condition_expression(self, expr: impl Into<String>) -> Self {
        Self {
            inner: self.inner.key_condition_expression(expr),
        }
    }

    #[sig(fn(QueryBuilder[@b], _) -> QueryBuilder[b.has_table, b.has_key_condition])]
    pub fn filter_expression(self, expr: impl Into<String>) -> Self {
        Self {
            inner: self.inner.filter_expression(expr),
        }
    }

    #[sig(fn(QueryBuilder[@b], _, _) -> QueryBuilder[b.has_table, b.has_key_condition])]
    pub fn expression_attribute_names(
        self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            inner: self.inner.expression_attribute_names(key, value),
        }
    }

    #[sig(fn(QueryBuilder[@b], HashMap<String, String>) -> QueryBuilder[b.has_table, b.has_key_condition])]
    pub fn set_expression_attribute_names(self, names: HashMap<String, String>) -> Self {
        Self {
            inner: self.inner.set_expression_attribute_names(Some(names)),
        }
    }

    #[sig(fn(QueryBuilder[@b], _, AttributeVal) -> QueryBuilder[b.has_table, b.has_key_condition])]
    pub fn expression_attribute_values(self, key: impl Into<String>, value: AttributeVal) -> Self {
        Self {
            inner: self.inner.expression_attribute_values(key, value.into()),
        }
    }

    #[sig(fn(QueryBuilder[@b], HashMap<String, AttributeVal>) -> QueryBuilder[b.has_table, b.has_key_condition])]
    pub fn set_expression_attribute_values(self, values: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            values.into_iter().map(|(k, v)| (k, v.into())).collect();
        Self {
            inner: self.inner.set_expression_attribute_values(Some(converted)),
        }
    }

    #[sig(fn(QueryBuilder[true, true]) -> _)]
    pub async fn send(
        self,
    ) -> Result<
        QueryOutput,
        aws_sdk_dynamodb::error::SdkError<aws_sdk_dynamodb::operation::query::QueryError>,
    > {
        let result = self.inner.send().await?;
        Ok(QueryOutput { inner: result })
    }
}

#[opaque]
pub struct QueryOutput {
    inner: aws_sdk_dynamodb::operation::query::QueryOutput,
}

#[trusted]
impl QueryOutput {
    #[sig(fn(&QueryOutput) -> Vec<HashMap<String, AttributeVal>>)]
    pub fn items(&self) -> Vec<HashMap<String, AttributeVal>> {
        self.inner
            .items()
            .iter()
            .map(|item| {
                item.iter()
                    .map(|(k, v)| (k.clone(), v.clone().into()))
                    .collect()
            })
            .collect()
    }

    #[sig(fn(&QueryOutput) -> i32)]
    pub fn count(&self) -> i32 {
        self.inner.count()
    }

    #[sig(fn(&QueryOutput) -> Option<HashMap<String, AttributeVal>>)]
    pub fn last_evaluated_key(&self) -> Option<HashMap<String, AttributeVal>> {
        self.inner.last_evaluated_key().map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.clone().into()))
                .collect()
        })
    }

    #[sig(fn(QueryOutput) -> Vec<HashMap<String, AttributeVal>>)]
    pub fn into_items(self) -> Vec<HashMap<String, AttributeVal>> {
        self.inner
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|item| item.into_iter().map(|(k, v)| (k, v.into())).collect())
            .collect()
    }
}
