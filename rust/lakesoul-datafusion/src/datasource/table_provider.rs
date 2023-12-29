// SPDX-FileCopyrightText: 2023 LakeSoul Contributors
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::any::Any;

use arrow::datatypes::SchemaRef;

use async_trait::async_trait;

use datafusion::catalog::schema::SchemaProvider;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::TableProvider;
use datafusion::catalog::CatalogList;
use datafusion::datasource::file_format::FileFormat;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::error::Result;
use datafusion::logical_expr::{TableType, TableProviderFilterPushDown};

use datafusion::physical_plan::ExecutionPlan;
use datafusion::{execution::context::SessionState, logical_expr::Expr};

use lakesoul_io::datasource::file_format::LakeSoulParquetFormat;
use lakesoul_io::datasource::listing::LakeSoulListingTable;
use lakesoul_io::lakesoul_io_config::LakeSoulIOConfig;
use proto::proto::entity::TableInfo;

use crate::catalog::parse_table_info_partitions;
use crate::serialize::arrow_java::schema_from_metadata_str;

use super::file_format::LakeSoulMetaDataParquetFormat;

use dashmap::DashMap;
use datafusion::error::DataFusionError;

/// Reads data from LakeSoul
///
/// # Features
///
/// 1. Merges schemas if the files have compatible but not indentical schemas
///
/// 2. Hive-style partitioning support, where a path such as
/// `/files/date=1/1/2022/data.parquet` is injected as a `date` column.
///
/// 3. Projection pushdown for formats that support it such as such as
/// Parquet
///
/// ```
pub struct LakeSoulTableProvider {
    listing_table: Arc<LakeSoulListingTable>, 
    table_info: Arc<TableInfo>,
    schema: SchemaRef,
    primary_keys: Vec<String>,
}

impl LakeSoulTableProvider {
    pub async fn try_new(
        session_state: &SessionState,
        lakesoul_io_config: LakeSoulIOConfig,
        table_info: Arc<TableInfo>,
        as_sink: bool
    ) -> crate::error::Result<Self> {
        let schema = schema_from_metadata_str(&table_info.table_schema);
        let (_, hash_partitions) = parse_table_info_partitions(table_info.partitions.clone());

        let file_format: Arc<dyn FileFormat> = match as_sink {
            true => Arc::new(LakeSoulMetaDataParquetFormat::new(
                    Arc::new(ParquetFormat::new()),
                    table_info.clone()
                ).await?),
            false => Arc::new(LakeSoulParquetFormat::new(
                Arc::new(ParquetFormat::new()), 
                lakesoul_io_config.clone()))
        };
        Ok(Self {
            listing_table: Arc::new(LakeSoulListingTable::new_with_config_and_format(
                session_state, 
                lakesoul_io_config, 
                file_format,
                as_sink
            ).await?),
            table_info,
            schema,
            primary_keys: hash_partitions,
        })
    }
    
    fn primary_keys(&self) -> &[String] {
        &self.primary_keys
    }

    fn table_info(&self) -> Arc<TableInfo> {
        self.table_info.clone()
    }
}


#[async_trait]
impl TableProvider for LakeSoulTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &SessionState,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table.scan(state, projection, filters, limit).await
    }

    fn supports_filter_pushdown(
        &self,
        filter: &Expr,
    ) -> Result<TableProviderFilterPushDown> {
        if self.primary_keys().is_empty() {
            Ok(TableProviderFilterPushDown::Exact)
        } else {
            if let Ok(cols) = filter.to_columns() {
                if cols.iter().all(|col| self.primary_keys().contains(&col.name)) {
                    Ok(TableProviderFilterPushDown::Inexact)
                } else {
                    Ok(TableProviderFilterPushDown::Unsupported)
                }
            } else {
                Ok(TableProviderFilterPushDown::Unsupported)
            }
        }
    }

    async fn insert_into(
        &self,
        state: &SessionState,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.listing_table.insert_into(state, input, overwrite).await
    }
}

#[derive(Default)]
pub struct LakeSoulSchemaProvider {
    tables: DashMap<String, Arc<dyn TableProvider>>,
}

#[async_trait]
impl SchemaProvider for LakeSoulSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.tables
            .iter()
            .map(|table| table.key().clone())
            .collect()
    }

    async fn table(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).map(|table| table.value().clone())
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> Result<Option<Arc<dyn TableProvider>>> {
        if self.table_exist(name.as_str()) {
            return Err(DataFusionError::Execution(format!(
                "The table {name} already exists"
            )));
        }
        Ok(self.tables.insert(name, table))
    }

    fn deregister_table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        Ok(self.tables.remove(name).map(|(_, table)| table))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

}

#[derive(Default)]
pub struct LakeSoulCatalogProvider {
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl CatalogProvider for LakeSoulCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        self.schemas.iter().map(|s| s.key().clone()).collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas.get(name).map(|s| s.value().clone())
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.into(), schema))
    }

    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        // cascade` is not used here, but can be used to control whether
        // to delete all tables in the schema or not.
        if let Some(schema) = self.schema(name) {
            let (_, removed) = self.schemas.remove(name).unwrap();
            Ok(Some(removed))
        } else {
            Ok(None)
        }
    }
}

#[derive(Default)]
pub struct LakeSoulCatalogList {
    /// Collection of catalogs containing schemas and ultimately TableProviders
    pub catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl CatalogList for LakeSoulCatalogList {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs.iter().map(|c| c.key().clone()).collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.get(name).map(|c| c.value().clone())
    }
}