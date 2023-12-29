mod test_sql {
    use std::ops::Deref;
    use std::sync::Arc;

    use std::env;
    use std::path::PathBuf;
    use std::time::SystemTime;
    use std::thread;
    use std::time::Duration;
    use chrono::naive::NaiveDate;

    use datafusion::catalog::CatalogProvider;
    use datafusion::catalog::schema::SchemaProvider;
    use lakesoul_io::filter::parser::Parser;

    use arrow::datatypes::DataType;

    use arrow::util::pretty::print_batches;
    use arrow::datatypes::{Schema, SchemaRef, Field, TimestampMicrosecondType, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use arrow::array::{ArrayRef, Int32Array, StringArray, TimestampMicrosecondArray};

    use datafusion::{assert_batches_eq, dataframe};
    use datafusion::prelude::DataFrame;
    use datafusion::logical_expr::LogicalPlanBuilder;
    use datafusion::logical_expr::col;
    use datafusion::logical_expr::Expr;
    use crate::datasource::table_provider::{LakeSoulCatalogProvider, LakeSoulTableProvider, LakeSoulSchemaProvider};
    use crate::error::Result;
    use crate::lakesoul_table::LakeSoulTable;

    //my 
    use datafusion::{execution::context::{SessionContext, SessionState}, datasource::TableProvider};



    use lakesoul_io::lakesoul_io_config::{LakeSoulIOConfigBuilder, create_session_context};

    use lakesoul_metadata::{MetaDataClient, MetaDataClientRef};

    use crate::catalog::{create_io_config_builder, create_table};

    async fn init_table(batch: RecordBatch, table_name: &str, schema: SchemaRef,  pks:Vec<String>, client: MetaDataClientRef) -> Result<()> {
        let builder = LakeSoulIOConfigBuilder::new()
                .with_schema(schema)
                .with_primary_keys(pks);
        create_table(client.clone(), table_name, builder.build()).await?;
        let lakesoul_table = LakeSoulTable::for_name(table_name).await?;
        lakesoul_table.execute_upsert(batch).await
    }

    fn create_batch_i32(names: Vec<&str>, values: Vec<&[i32]>) -> RecordBatch {
        let values = values
            .into_iter()
            .map(|vec| Arc::new(Int32Array::from(Vec::from(vec))) as ArrayRef)
            .collect::<Vec<ArrayRef>>();
        let iter = names.into_iter().zip(values).map(|(name, array)| (name, array, true)).collect::<Vec<_>>();
        RecordBatch::try_from_iter_with_nullable(iter).unwrap()
    }

    async fn test_show_database () -> Result<()>{
        let table_name = "test_show_database";
        let client = Arc::new(MetaDataClient::from_env().await?);
        init_table(
            create_batch_i32(vec!["range", "hash", "value"], vec![&[20201101, 20201101, 20201101, 20201102], &[1, 2, 3, 4], &[1, 2, 3, 4]]),
             table_name, 
             SchemaRef::new(Schema::new(["range", "hash", "value"].into_iter().map(|name| Field::new(name, DataType::Int32, true)).collect::<Vec<Field>>())),
             vec!["range".to_string(), "hash".to_string()],
             client.clone(),
        ).await?;
        
        let lakesoul_table: LakeSoulTable = LakeSoulTable::for_name(table_name).await?;
        let builder = create_io_config_builder(client.clone(), None, false).await?;
        let ctx = create_session_context(&mut builder.clone().build())?;
        let builder=create_io_config_builder(client,Some(table_name),true).await?;
        let provider = Arc::new(LakeSoulTableProvider::try_new(&ctx.state(), builder.build(), lakesoul_table.table_info(), true).await?);
        // let df=ctx.read_table(provider)?;
        let _=ctx.register_table("test_show_database",provider.clone());
        let schema_provider = Arc::new(LakeSoulSchemaProvider::default());
        let _=schema_provider.register_table("domain".to_string(), provider);
        let catalog_provider=Arc::new(LakeSoulCatalogProvider::default());
        let _=catalog_provider.register_schema("default", schema_provider);
        // let df=ctx.sql("SELECT * FROM test_show_database").await?;
        let df=ctx.sql("SHOW TABLES").await?;
        // df.show().await?;
        Ok(())
    }





    #[tokio::test]
    async fn test_metadata_cases()  -> Result<()> {
        test_show_database().await?;

        Ok(())
    }
}