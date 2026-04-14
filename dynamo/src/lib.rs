use std::collections::HashMap;

use flux_rs::attrs::*;

extern crate flux_alloc;

// ==========================================================================
// AttributeVal — opaque wrapper around aws_sdk_dynamodb::types::AttributeValue
// ==========================================================================

#[opaque]
pub struct AttributeVal {
    inner: aws_sdk_dynamodb::types::AttributeValue,
}

#[trusted]
impl AttributeVal {
    #[sig(fn(String) -> AttributeVal)]
    pub fn s(value: String) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::S(value),
        }
    }

    #[sig(fn(String) -> AttributeVal)]
    pub fn n(value: String) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::N(value),
        }
    }

    #[sig(fn(bool) -> AttributeVal)]
    pub fn bool(value: bool) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::Bool(value),
        }
    }

    #[sig(fn(bool) -> AttributeVal)]
    pub fn null(value: bool) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::Null(value),
        }
    }

    #[sig(fn(Vec<String>) -> AttributeVal)]
    pub fn ss(value: Vec<String>) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::Ss(value),
        }
    }

    #[sig(fn(Vec<String>) -> AttributeVal)]
    pub fn ns(value: Vec<String>) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::Ns(value),
        }
    }

    #[sig(fn(Vec<AttributeVal>) -> AttributeVal)]
    pub fn l(value: Vec<AttributeVal>) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::L(
                value.into_iter().map(|v| v.inner).collect(),
            ),
        }
    }

    #[sig(fn(HashMap<String, AttributeVal>) -> AttributeVal)]
    pub fn m(value: HashMap<String, AttributeVal>) -> Self {
        Self {
            inner: aws_sdk_dynamodb::types::AttributeValue::M(
                value.into_iter().map(|(k, v)| (k, v.inner)).collect(),
            ),
        }
    }

    #[sig(fn(&AttributeVal) -> bool)]
    pub fn is_s(&self) -> bool {
        self.inner.is_s()
    }

    #[sig(fn(&AttributeVal) -> Option<&String>)]
    pub fn as_s(&self) -> Option<&String> {
        self.inner.as_s().ok()
    }

    #[sig(fn(&AttributeVal) -> bool)]
    pub fn is_n(&self) -> bool {
        self.inner.is_n()
    }

    #[sig(fn(&AttributeVal) -> Option<&String>)]
    pub fn as_n(&self) -> Option<&String> {
        self.inner.as_n().ok()
    }

    #[sig(fn(&AttributeVal) -> bool)]
    pub fn is_bool(&self) -> bool {
        self.inner.is_bool()
    }

    #[sig(fn(&AttributeVal) -> Option<bool>)]
    pub fn as_bool(&self) -> Option<bool> {
        self.inner.as_bool().ok().copied()
    }

    #[sig(fn(AttributeVal) -> aws_sdk_dynamodb::types::AttributeValue)]
    pub fn into_inner(self) -> aws_sdk_dynamodb::types::AttributeValue {
        self.inner
    }

    #[sig(fn(aws_sdk_dynamodb::types::AttributeValue) -> AttributeVal)]
    pub fn from_inner(inner: aws_sdk_dynamodb::types::AttributeValue) -> Self {
        Self { inner }
    }
}

#[trusted]
impl From<aws_sdk_dynamodb::types::AttributeValue> for AttributeVal {
    fn from(inner: aws_sdk_dynamodb::types::AttributeValue) -> Self {
        Self { inner }
    }
}

#[trusted]
impl From<AttributeVal> for aws_sdk_dynamodb::types::AttributeValue {
    fn from(val: AttributeVal) -> Self {
        val.inner
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
            inner: self.inner.key(key, value.inner),
        }
    }

    #[sig(fn(GetItemBuilder[@b], HashMap<String, AttributeVal>) -> GetItemBuilder[b.has_table, true])]
    pub fn set_key(self, key: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            key.into_iter().map(|(k, v)| (k, v.inner)).collect();
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
        self.inner.item.map(|m| {
            m.into_iter()
                .map(|(k, v)| (k, AttributeVal { inner: v }))
                .collect()
        })
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
            inner: self.inner.item(key, value.inner),
        }
    }

    #[sig(fn(PutItemBuilder[@b], HashMap<String, AttributeVal>[@m]) -> PutItemBuilder{b_new: b_new.table_name == b.table_name })]
    pub fn set_item(self, item: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            item.into_iter().map(|(k, v)| (k, v.inner)).collect();
        Self {
            inner: self.inner.set_item(Some(converted)),
        }
    }

    #[sink]
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
        self.inner.attributes.map(|m| {
            m.into_iter()
                .map(|(k, v)| (k, AttributeVal { inner: v }))
                .collect()
        })
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
            inner: self.inner.key(key, value.inner),
        }
    }

    #[sig(fn(DeleteItemBuilder[@b], HashMap<String, AttributeVal>) -> DeleteItemBuilder[b.has_table, true])]
    pub fn set_key(self, key: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            key.into_iter().map(|(k, v)| (k, v.inner)).collect();
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
        self.inner.attributes.map(|m| {
            m.into_iter()
                .map(|(k, v)| (k, AttributeVal { inner: v }))
                .collect()
        })
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
            inner: self.inner.key(key, value.inner),
        }
    }

    #[sig(fn(UpdateItemBuilder[@b], HashMap<String, AttributeVal>) -> UpdateItemBuilder[b.has_table, true])]
    pub fn set_key(self, key: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            key.into_iter().map(|(k, v)| (k, v.inner)).collect();
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
            inner: self.inner.expression_attribute_values(key, value.inner),
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
        self.inner.attributes.map(|m| {
            m.into_iter()
                .map(|(k, v)| (k, AttributeVal { inner: v }))
                .collect()
        })
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
            inner: self.inner.expression_attribute_values(key, value.inner),
        }
    }

    #[sig(fn(QueryBuilder[@b], HashMap<String, AttributeVal>) -> QueryBuilder[b.has_table, b.has_key_condition])]
    pub fn set_expression_attribute_values(self, values: HashMap<String, AttributeVal>) -> Self {
        let converted: HashMap<String, aws_sdk_dynamodb::types::AttributeValue> =
            values.into_iter().map(|(k, v)| (k, v.inner)).collect();
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
                    .map(|(k, v)| (k.clone(), AttributeVal { inner: v.clone() }))
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
                .map(|(k, v)| (k.clone(), AttributeVal { inner: v.clone() }))
                .collect()
        })
    }

    #[sig(fn(QueryOutput) -> Vec<HashMap<String, AttributeVal>>)]
    pub fn into_items(self) -> Vec<HashMap<String, AttributeVal>> {
        self.inner
            .items
            .unwrap_or_default()
            .into_iter()
            .map(|item| {
                item.into_iter()
                    .map(|(k, v)| (k, AttributeVal { inner: v }))
                    .collect()
            })
            .collect()
    }
}
